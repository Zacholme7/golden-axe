use deadpool_postgres::Pool;
use handlebars::{self, Handlebars};
use itertools::Itertools;
use shared::jrpc;
use std::{collections::HashMap, fmt, sync::Arc};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use url::Url;

use alloy::primitives::{BlockHash, FixedBytes, U16, U256, U64};
use eyre::{eyre, Context, Result};
use futures::pin_mut;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};

use crate::{api, broadcast};

#[derive(Debug)]
pub enum Error {
    Wait,
    Retry(String),
    Fatal(eyre::Report),
}

impl From<eyre::Report> for Error {
    fn from(err: eyre::Report) -> Self {
        Self::Fatal(err)
    }
}

impl From<jrpc::Error> for Error {
    fn from(err: jrpc::Error) -> Self {
        if err.message == "no result" {
            Self::Wait
        } else {
            Self::Retry(format!("jrpc error {err:?}"))
        }
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Self::Fatal(eyre!("database-error={}", err.to_string()))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RemoteConfig {
    pub enabled: bool,
    pub chain: u64,
    pub url: Url,
    pub start_block: Option<i64>,
    pub batch_size: u16,
    pub concurrency: u16,
}

impl fmt::Display for RemoteConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemoteConfig({}, {}, enabled={})",
            self.chain, self.url, self.enabled
        )
    }
}

impl RemoteConfig {
    pub async fn load(pool: &Pool) -> Result<Vec<RemoteConfig>> {
        Ok(pool
            .get()
            .await?
            .query(
                "select enabled, chain, url, start_block, batch_size, concurrency from config",
                &[],
            )
            .await?
            .iter()
            .map(|row| RemoteConfig {
                enabled: row.get("enabled"),
                chain: row.get::<&str, U64>("chain").to(),
                url: row
                    .get::<&str, String>("url")
                    .parse()
                    .expect("unable to parse url"),
                start_block: row.get("start_block"),
                batch_size: row.get::<&str, U16>("batch_size").to(),
                concurrency: row.get::<&str, U16>("concurrency").to(),
            })
            .collect_vec())
    }
}

pub async fn test(url: &str, chain: u64) -> Result<(), shared::Error> {
    let parsed: Url = url.parse().wrap_err("unable to parse rpc url")?;
    let jrpc_client = jrpc::Client::new(parsed.as_str());
    match jrpc_client.chain_id().await {
        Err(e) => Err(shared::Error::User(format!("rpc error {e}"))),
        Ok(resp) if resp.to::<u64>() == chain => Ok(()),
        Ok(id) => Err(shared::Error::User(format!(
            "expected chain {chain} got {id}",
        ))),
    }
}

pub async fn run(config: api::Config) {
    let mut table: HashMap<RemoteConfig, JoinHandle<()>> = HashMap::new();
    loop {
        let remotes = RemoteConfig::load(&config.fe_pool)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("loading remote config {}", e);
                vec![]
            })
            .into_iter()
            .filter(|rc| rc.enabled)
            .collect_vec();
        for remote in remotes.iter() {
            if !table.contains_key(remote) {
                let (conf, be_pool, broadcaster) = (
                    remote.clone(),
                    config.be_pool.clone(),
                    config.broadcaster.clone(),
                );
                table.insert(
                    conf.clone(),
                    tokio::spawn(
                        async move { Downloader::new(conf, be_pool, broadcaster).run().await },
                    ),
                );
            }
        }
        for key in table.keys().cloned().collect_vec() {
            if let Some(handle) = table.get_mut(&key) {
                if !remotes.iter().any(|rc| rc.eq(&key)) {
                    tracing::error!("aborting {}", key);
                    handle.abort();
                }
                if handle.is_finished() {
                    match handle.await {
                        Ok(_) => tracing::info!("finished {}", key),
                        Err(e) => tracing::error!("{} {:?}", key, e),
                    }
                    table.remove(&key);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub struct Downloader {
    pub chain: api::Chain,
    pub batch_size: u16,
    pub concurrency: u16,
    pub start_block: Option<i64>,

    be_pool: Pool,
    jrpc_client: Arc<jrpc::Client>,
    broadcaster: Arc<broadcast::Channel>,
    partition_max_block: Option<u64>,
}

impl Downloader {
    pub fn new(
        config: RemoteConfig,
        be_pool: Pool,
        broadcaster: Arc<broadcast::Channel>,
    ) -> Downloader {
        let jrpc_client = Arc::new(jrpc::Client::new(config.url.as_ref()));
        Downloader {
            chain: config.chain.into(),
            batch_size: config.batch_size,
            concurrency: config.concurrency,
            start_block: config.start_block,
            be_pool,
            jrpc_client,
            broadcaster,
            partition_max_block: None,
        }
    }

    async fn init_blocks(&mut self) -> Result<(), Error> {
        if !self
            .be_pool
            .get()
            .await
            .wrap_err("getting pg")?
            .query(
                "select true from blocks where chain = $1 limit 1",
                &[&self.chain],
            )
            .await
            .expect("unable to query for latest block")
            .is_empty()
        {
            return Ok(());
        }
        let block = match self.start_block {
            Some(n) => self.jrpc_client.block(format!("0x{:x}", n)).await?,
            None => self.jrpc_client.block("latest".to_string()).await?,
        };
        let mut blocks = vec![block];
        let from = blocks[0].number.to();
        tracing::info!("initializing blocks table at: {}", blocks[0].number);
        let mut logs = self.jrpc_client.logs(from, from).await?;
        add_timestamp(&mut blocks, &mut logs);
        let receipts = self.jrpc_client.receipts(&tx_hashes(&blocks)).await?;
        validate_logs(&blocks, &logs)?;
        validate_receipts(&blocks, &receipts)?;
        let mut pg = self.be_pool.get().await.wrap_err("getting pg")?;
        let pgtx = pg.transaction().await?;
        self.partition_max_block =
            setup_tables(&pgtx, self.chain.0, from, self.partition_max_block)
                .await
                .expect("setting up table for initial block");
        copy_logs(&pgtx, self.chain, logs).await?;
        copy_erc20_transfers_from_logs(&pgtx, self.chain, from, from).await?;
        copy_txs(&pgtx, self.chain, &blocks).await?;
        copy_receipts(&pgtx, self.chain, &receipts).await?;
        copy_blocks(&pgtx, self.chain, &blocks).await?;
        pgtx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(skip_all fields(event, chain = self.chain.0))]
    pub async fn run(mut self) {
        if let Err(e) = self.init_blocks().await {
            tracing::error!("init {:?}", e);
            return;
        }
        let configured_batch_size = self.batch_size;
        let mut batch_size = configured_batch_size;
        loop {
            match self.download(batch_size).await {
                Err(Error::Wait) => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(Error::Retry(err)) => {
                    let next_batch_size = std::cmp::max(1, batch_size / 10);
                    tracing::error!(
                        current_batch_size = batch_size,
                        next_batch_size,
                        "downloading error: {}",
                        err
                    );
                    batch_size = next_batch_size;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(Error::Fatal(err)) => {
                    tracing::error!("fatal downloading error: {}", err);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Ok(last) => {
                    self.broadcaster.update(self.chain.0);
                    let _ = self.broadcaster.json_updates.send(serde_json::json!({
                        "new_block": "local",
                        "chain": self.chain.0,
                        "num": last,
                    }));
                    if batch_size < configured_batch_size {
                        batch_size = std::cmp::min(
                            configured_batch_size,
                            batch_size.saturating_mul(2),
                        );
                    }
                }
            }
        }
    }

    async fn delete_after(&self, n: u64) -> Result<(), Error> {
        let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        pgtx.execute(
            "delete from blocks where chain = $1 and num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.execute(
            "delete from logs where chain = $1 and block_num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.execute(
            "delete from txs where chain = $1 and block_num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.execute(
            "delete from receipts where chain = $1 and block_num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.execute(
            "delete from erc20_transfers where chain = $1 and block_num >= $2",
            &[&self.chain, &U64::from(n)],
        )
        .await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        Ok(())
    }

    /*
    Request remote latest n and local latest k.
    If k == n-1 we simply can download logs for block n-1
    If k <  n-2 we will make n-1-k requests to download n-1-k blocks
    and another request to download n-1-k logs.

    After downloading n-k blocks/logs we check n's parent hash with k's hash.
    If the hashes aren't equal we delete k's block/logs
    and start the process over again.

    If the hashes match, we copy blocks, transactions, receipts, and logs into their tables
    */
    #[tracing::instrument(level="info" skip_all, fields(from, to, blocks, txs, logs, receipts, erc20_transfers))]
    async fn download(&mut self, batch_size: u16) -> Result<u64, Error> {
        let latest = self.jrpc_client.block("latest".to_string()).await?;
        let _ = self.broadcaster.json_updates.send(serde_json::json!({
            "new_block": "remote",
            "chain": self.chain.0,
            "num": latest.number.to::<u64>(),
        }));
        let (local_num, local_hash) = self.local_latest().await?;
        if local_num >= latest.number.to() {
            return Err(Error::Wait);
        }

        let delta = latest.number.to::<u64>() - local_num;
        let (from, to) = (local_num + 1, local_num + delta.min(batch_size as u64));
        {
            let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
            let pgtx = pg.transaction().await?;
            self.partition_max_block =
                setup_tables(&pgtx, self.chain.0, to, self.partition_max_block).await?;
            pgtx.commit().await.wrap_err("unable to commit tx")?;
        }
        tracing::Span::current()
            .record("from", from)
            .record("to", to);
        let (blocks_res, logs_res) = tokio::join!(
            self.jrpc_client.blocks(from, to),
            self.jrpc_client.logs(from, to),
        );
        let (mut blocks, mut logs) = (blocks_res?, logs_res?);
        add_timestamp(&mut blocks, &mut logs);
        let receipts = self.jrpc_client.receipts(&tx_hashes(&blocks)).await?;
        validate_blocks(from, to, &blocks)?;
        validate_logs(&blocks, &logs)?;
        validate_receipts(&blocks, &receipts)?;
        let (first_block, last_block) = (blocks.first().unwrap(), blocks.last().unwrap());

        if first_block.parent_hash != local_hash {
            self.delete_after(local_num).await?;
            return Err(Error::Fatal(eyre!("reorg")));
        }
        let mut pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let pgtx = pg.transaction().await?;
        let num_logs = copy_logs(&pgtx, self.chain, logs).await?;
        let num_erc20_transfers =
            copy_erc20_transfers_from_logs(&pgtx, self.chain, from, to).await?;
        let num_txs = copy_txs(&pgtx, self.chain, &blocks).await?;
        let num_receipts = copy_receipts(&pgtx, self.chain, &receipts).await?;
        let num_blocks = copy_blocks(&pgtx, self.chain, &blocks).await?;
        pgtx.commit().await.wrap_err("unable to commit tx")?;
        tracing::Span::current()
            .record("blocks", num_blocks)
            .record("logs", num_logs)
            .record("erc20_transfers", num_erc20_transfers)
            .record("txs", num_txs)
            .record("receipts", num_receipts);
        Ok(last_block.number.to())
    }

    async fn local_latest(&self) -> Result<(u64, BlockHash), Error> {
        let pg = self.be_pool.get().await.wrap_err("pg pool")?;
        let q = "SELECT num, hash from blocks where chain = $1 order by num desc limit 1";
        let row = pg
            .query_one(q, &[&self.chain])
            .await
            .wrap_err("getting local latest")?;
        Ok((
            row.try_get::<&str, i64>("num")? as u64,
            row.try_get("hash")?,
        ))
    }
}

pub async fn sync_one(
    pg: &mut tokio_postgres::Client,
    client: &jrpc::Client,
    chain: u64,
    n: u64,
) -> Result<u64, Error> {
    let mut blocks = client.blocks(n, n).await?;
    let mut logs = client.logs(n, n).await?;
    add_timestamp(&mut blocks, &mut logs);
    validate_blocks(n, n, &blocks)?;

    let pgtx = pg.transaction().await?;
    let num_logs = copy_logs(&pgtx, api::Chain(chain), logs).await?;
    copy_erc20_transfers_from_logs(&pgtx, api::Chain(chain), n, n).await?;
    pgtx.commit().await.wrap_err("unable to commit tx")?;
    Ok(num_logs)
}

fn validate_logs(blocks: &[jrpc::Block], logs: &[jrpc::Log]) -> Result<(), Error> {
    let mut logs_by_block: HashMap<U64, Vec<&jrpc::Log>> = HashMap::new();
    for log in logs {
        logs_by_block.entry(log.block_number).or_default().push(log);
    }
    for block in blocks {
        let has_logs = logs_by_block
            .get(&block.number)
            .is_some_and(|v| !v.is_empty());
        let has_bloom = block.logs_bloom != FixedBytes::<256>::ZERO;
        if !has_logs && has_bloom {
            return Err(Error::Fatal(eyre!("bloom without logs {}", block.number)));
        }
    }
    Ok(())
}

fn tx_hashes(blocks: &[jrpc::Block]) -> Vec<BlockHash> {
    blocks
        .iter()
        .flat_map(|block| block.transactions.iter().map(|tx| tx.hash))
        .collect()
}

fn validate_receipts(blocks: &[jrpc::Block], receipts: &[jrpc::Receipt]) -> Result<(), Error> {
    let txs = blocks
        .iter()
        .flat_map(|block| block.transactions.iter().map(move |tx| (block.number, tx)))
        .collect_vec();
    if txs.len() != receipts.len() {
        return Err(Error::Fatal(eyre!(
            "want {} receipts got {}",
            txs.len(),
            receipts.len()
        )));
    }
    for ((block_number, tx), receipt) in txs.into_iter().zip(receipts.iter()) {
        if receipt.tx_hash != tx.hash {
            return Err(Error::Fatal(eyre!(
                "receipt tx hash mismatch {} {}",
                receipt.tx_hash,
                tx.hash
            )));
        }
        if receipt.block_number != block_number {
            return Err(Error::Fatal(eyre!(
                "receipt block mismatch {} {}",
                receipt.block_number,
                block_number
            )));
        }
        if receipt.idx != tx.idx {
            return Err(Error::Fatal(eyre!(
                "receipt index mismatch {} {}",
                receipt.idx,
                tx.idx
            )));
        }
    }
    Ok(())
}

fn validate_blocks(from: u64, to: u64, blocks: &[jrpc::Block]) -> Result<(), Error> {
    if let Some(i) = blocks.first().map(|b| b.number.to::<u64>()) {
        if i != from {
            return Err(Error::Fatal(eyre!("want first block {} got {}", from, i)));
        }
    }
    if let Some(i) = blocks.last().map(|b| b.number.to::<u64>()) {
        if i != to {
            return Err(Error::Fatal(eyre!("want last block {} got {}", from, i)));
        }
    }
    for (prev, curr) in blocks.iter().zip(blocks.iter().skip(1)) {
        if curr.parent_hash != prev.hash {
            return Err(Error::Fatal(eyre!(
                "block {} has wrong parent_hash {} (expected {})",
                curr.number,
                curr.parent_hash,
                prev.hash
            )));
        }
    }
    Ok(())
}

//timescaledb
pub async fn setup_tables(
    pgtx: &Transaction<'_>,
    chain: u64,
    new_block: u64,
    partition_max_block: Option<u64>,
) -> Result<Option<u64>, Error> {
    const N: u64 = 2000000;
    let from = match partition_max_block {
        Some(max) if new_block < max + 1 => return Ok(partition_max_block),
        Some(max) => ((max + 1) / N) * N,
        None => (new_block / N) * N,
    };
    let to = from + N;
    let label = from / 1000000;
    let query = Handlebars::new()
        .render_template(
            "
            create table if not exists blocks_c{{chain}}
            partition of blocks
            for values in ({{chain}})
            partition by range (num);

            create table if not exists blocks_c{{chain}}_b{{label}}
            partition of blocks_c{{chain}}
            for values from ({{from}}) to ({{to}});

            create table if not exists txs_c{{chain}}
            partition of txs
            for values in ({{chain}})
            partition by range (block_num);

            create table if not exists txs_c{{chain}}_b{{label}}
            partition of txs_c{{chain}}
            for values from ({{from}}) to ({{to}});
            alter table txs_c{{chain}}_b{{label}} set (toast_tuple_target = 128);

            create table if not exists logs_c{{chain}}
            partition of logs
            for values in ({{chain}})
            partition by range (block_num);

            create table if not exists logs_c{{chain}}_b{{label}}
            partition of logs_c{{chain}}
            for values from ({{from}}) to ({{to}});
            alter table logs_c{{chain}}_b{{label}} set (toast_tuple_target = 128);

            create table if not exists receipts_c{{chain}}
            partition of receipts
            for values in ({{chain}})
            partition by range (block_num);

            create table if not exists receipts_c{{chain}}_b{{label}}
            partition of receipts_c{{chain}}
            for values from ({{from}}) to ({{to}});

            create table if not exists erc20_transfers_c{{chain}}
            partition of erc20_transfers
            for values in ({{chain}});
            ",
            &serde_json::json!({"chain": chain, "label": label, "from": from, "to": to,}),
        )
        .wrap_err("rendering sql template")?;
    tracing::info!("new table range label={} from={} to={}", label, from, to);
    pgtx.batch_execute(&query).await?;
    Ok(Some(to - 1))
}

pub async fn bootstrap_erc20_transfers(pool: &Pool) -> Result<()> {
    let pg = pool.get().await.wrap_err("getting pg")?;
    let transfer_topic = erc20_transfer_topic().to_vec();
    let row = pg
        .query_one(
            "
            select
                exists(
                    select 1
                    from logs l
                    where coalesce(array_length(l.topics, 1), 0) >= 3
                      and l.topics[1] = $1
                      and not exists (
                          select 1
                          from erc20_transfers e
                          where e.chain = l.chain
                            and e.tx_hash = l.tx_hash
                            and e.log_idx = l.log_idx
                      )
                    limit 1
                ) as needs_bootstrap
            ",
            &[&transfer_topic],
        )
        .await
        .wrap_err("checking erc20 bootstrap state")?;
    let needs_bootstrap: bool = row.get("needs_bootstrap");
    if !needs_bootstrap {
        return Ok(());
    }

    let chains = pg
        .query(
            "
            select chain, max(block_num)::bigint as max_block
            from logs
            group by chain
            order by chain asc
            ",
            &[],
        )
        .await
        .wrap_err("loading chains for erc20 bootstrap")?;
    drop(pg);

    for row in chains {
        let chain = row.get::<_, i64>("chain") as u64;
        let max_block = row.get::<_, i64>("max_block") as u64;
        let mut pg = pool.get().await.wrap_err("getting pg")?;
        let pgtx = pg.transaction().await.wrap_err("starting erc20 bootstrap tx")?;
        setup_tables(&pgtx, chain, max_block, None)
            .await
            .map_err(|error| eyre!("setting up erc20 bootstrap tables: {error:?}"))?;
        let inserted =
            copy_erc20_transfers_from_logs(&pgtx, api::Chain(chain), 0, max_block)
                .await
                .wrap_err("backfilling erc20 transfers")?;
        pgtx.commit()
            .await
            .wrap_err("committing erc20 bootstrap tx")?;
        tracing::info!(chain, max_block, inserted, "bootstrapped erc20 transfer ledger");
    }

    Ok(())
}

fn add_timestamp(blocks: &mut [jrpc::Block], logs: &mut Vec<jrpc::Log>) {
    for block in blocks.iter_mut() {
        for tx in block.transactions.iter_mut() {
            tx.block_timestamp = Some(block.timestamp);
        }
    }
    let indexed: HashMap<u64, &jrpc::Block> = blocks.iter().map(|b| (b.number.to(), b)).collect();
    for log in logs {
        if let Some(block) = indexed.get(&log.block_number.to()) {
            log.block_timestamp = Some(block.timestamp);
        }
    }
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_logs(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    logs: Vec<jrpc::Log>,
) -> Result<u64> {
    const Q: &str = "
        copy logs (
            chain,
            block_num,
            block_timestamp,
            log_idx,
            tx_hash,
            address,
            topics,
            data
        )
        from stdin binary
    ";
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TIMESTAMPTZ,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA_ARRAY,
            tokio_postgres::types::Type::BYTEA,
        ],
    );
    pin_mut!(writer);
    for log in logs {
        writer
            .as_mut()
            .write(&[
                &chain,
                &log.block_number,
                &OffsetDateTime::from_unix_timestamp(
                    log.block_timestamp.expect("missing log ts").to::<u64>() as i64,
                )?,
                &log.log_idx,
                &log.tx_hash,
                &log.address,
                &log.topics,
                &log.data.to_vec(),
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in logs")
}

pub async fn copy_erc20_transfers_from_logs(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    from: u64,
    to: u64,
) -> Result<u64> {
    let transfer_topic = erc20_transfer_topic().to_vec();
    pgtx.execute(
        r#"
        insert into erc20_transfers (
            chain,
            block_num,
            block_timestamp,
            log_idx,
            tx_hash,
            token_address,
            "from",
            "to",
            amount
        )
        select
            chain,
            block_num,
            block_timestamp,
            log_idx,
            tx_hash,
            address as token_address,
            abi_address(topics[2]) as "from",
            abi_address(topics[3]) as "to",
            abi_uint(data) as amount
        from logs
        where chain = $1
          and block_num >= $2
          and block_num <= $3
          and coalesce(array_length(topics, 1), 0) >= 3
          and topics[1] = $4
        on conflict (chain, tx_hash, log_idx) do nothing
        "#,
        &[&chain, &(from as i64), &(to as i64), &transfer_topic],
    )
    .await
    .wrap_err("unable to insert erc20 transfers from logs")
}

fn erc20_transfer_topic() -> BlockHash {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .parse()
        .expect("valid erc20 transfer topic")
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_txs(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    blocks: &[jrpc::Block],
) -> Result<u64> {
    const Q: &str = r#"
        copy txs (
            chain,
            block_num,
            block_timestamp,
            idx,
            type,
            gas,
            gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            hash,
            nonce,
            "from",
            "to",
            input,
            value,
            fee_token,
            calls
        )
        from stdin binary
    "#;
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TIMESTAMPTZ,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::INT2,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::JSONB,
        ],
    );
    pin_mut!(writer);
    for block in blocks {
        for tx in &block.transactions {
            let to = tx.to.as_ref().map(|address| address.to_vec());
            writer
                .as_mut()
                .write(&[
                    &chain,
                    &block.number,
                    &OffsetDateTime::from_unix_timestamp(
                        tx.block_timestamp.expect("missing tx ts").to::<u64>() as i64,
                    )?,
                    &tx.idx,
                    &tx.ty.unwrap_or(U64::from(0)),
                    &tx.gas,
                    &tx.gas_price.unwrap_or(U256::from(0)),
                    &tx.max_fee_per_gas,
                    &tx.max_priority_fee_per_gas,
                    &tx.hash,
                    &tx.nonce,
                    &tx.from.to_vec(),
                    &to,
                    &tx.input.to_vec(),
                    &tx.value,
                    &tx.fee_token.as_ref().map(|a| a.to_vec()),
                    &tx.calls
                        .as_ref()
                        .filter(|v| !v.is_empty())
                        .map(|v| tokio_postgres::types::Json(v.as_slice())),
                ])
                .await?;
        }
    }
    writer.finish().await.wrap_err("unable to copy in txs")
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_receipts(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    receipts: &[jrpc::Receipt],
) -> Result<u64> {
    const Q: &str = r#"
        copy receipts (
            chain,
            block_num,
            idx,
            tx_hash,
            status,
            gas_used,
            cumulative_gas_used,
            effective_gas_price,
            contract_address
        )
        from stdin binary
    "#;
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BOOL,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::BYTEA,
        ],
    );
    pin_mut!(writer);
    for receipt in receipts {
        let status = receipt.status.map(|status| status != U64::from(0));
        let contract_address = receipt.contract_address.as_ref().map(|address| address.to_vec());
        writer
            .as_mut()
            .write(&[
                &chain,
                &receipt.block_number,
                &receipt.idx,
                &receipt.tx_hash,
                &status,
                &receipt.gas_used,
                &receipt.cumulative_gas_used,
                &receipt.effective_gas_price,
                &contract_address,
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in receipts")
}

#[tracing::instrument(level="debug" fields(chain) skip_all)]
pub async fn copy_blocks(
    pgtx: &Transaction<'_>,
    chain: api::Chain,
    blocks: &[jrpc::Block],
) -> Result<u64> {
    const Q: &str = r#"
        copy blocks (
            chain,
            num,
            timestamp,
            size,
            gas_limit,
            gas_used,
            base_fee_per_gas,
            hash,
            parent_hash,
            nonce,
            receipts_root,
            state_root,
            extra_data,
            miner
        )
        from stdin binary
    "#;
    let sink = pgtx.copy_in(Q).await.expect("unable to start copy in");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TIMESTAMPTZ,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::NUMERIC,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
        ],
    );
    pin_mut!(writer);
    for block in blocks {
        writer
            .as_mut()
            .write(&[
                &chain,
                &block.number,
                &OffsetDateTime::from_unix_timestamp(block.timestamp.to())?,
                &block.size,
                &block.gas_limit,
                &block.gas_used,
                &block.base_fee_per_gas,
                &block.hash,
                &block.parent_hash,
                &block.nonce,
                &block.receipts_root,
                &block.state_root,
                &block.extra_data.to_vec(),
                &block.miner.to_vec(),
            ])
            .await?;
    }
    writer.finish().await.wrap_err("unable to copy in blocks")
}

#[cfg(test)]
mod tests {
    use alloy::{
        hex,
        primitives::{Address, Bytes, FixedBytes, B256, U256, U64},
    };

    use crate::api;

    use super::{
        copy_blocks, copy_erc20_transfers_from_logs, copy_logs, copy_receipts, copy_txs,
        erc20_transfer_topic, setup_tables,
    };
    use shared::jrpc;

    static SCHEMA_BE: &str = include_str!("./sql/schema.sql");

    fn test_block() -> jrpc::Block {
        jrpc::Block {
            hash: B256::with_last_byte(0x10),
            parent_hash: B256::with_last_byte(0x0f),
            number: U64::from(1),
            nonce: U256::from(2),
            timestamp: U64::from(1_700_000_000),
            size: U64::from(128),
            transactions: vec![jrpc::Tx {
                ty: Some(U64::from(2)),
                hash: B256::with_last_byte(0x20),
                block_timestamp: Some(U64::from(1_700_000_000)),
                idx: U64::from(0),
                nonce: U256::from(7),
                from: Address::with_last_byte(0x21),
                to: None,
                input: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
                value: U256::from(3),
                gas: U256::from(21_000),
                gas_price: Some(U256::from(42)),
                max_fee_per_gas: Some(U256::from(60)),
                max_priority_fee_per_gas: Some(U256::from(2)),
                calls: None,
                fee_token: None,
            }],
            gas_limit: U256::from(30_000_000u64),
            gas_used: U256::from(21_000),
            base_fee_per_gas: Some(U256::from(40)),
            logs_bloom: FixedBytes::<256>::ZERO,
            receipts_root: B256::with_last_byte(0x30),
            state_root: B256::with_last_byte(0x31),
            extra_data: Bytes::from(vec![0x01, 0x02]),
            miner: Address::with_last_byte(0x32),
        }
    }

    fn test_receipt(block: &jrpc::Block) -> jrpc::Receipt {
        jrpc::Receipt {
            tx_hash: block.transactions[0].hash,
            idx: block.transactions[0].idx,
            block_number: block.number,
            status: Some(U64::from(1)),
            gas_used: U256::from(21_000),
            cumulative_gas_used: U256::from(21_000),
            effective_gas_price: Some(U256::from(41)),
            contract_address: Some(Address::with_last_byte(0x33)),
        }
    }

    fn test_transfer_log(block: &jrpc::Block) -> jrpc::Log {
        let from = Address::with_last_byte(0x44);
        let to = Address::with_last_byte(0x55);
        let mut from_topic = FixedBytes::<32>::ZERO;
        from_topic[12..32].copy_from_slice(from.as_slice());
        let mut to_topic = FixedBytes::<32>::ZERO;
        to_topic[12..32].copy_from_slice(to.as_slice());
        let mut amount = vec![0u8; 31];
        amount.push(0x7b);

        jrpc::Log {
            block_number: block.number,
            block_timestamp: Some(U64::from(1_700_000_000)),
            tx_hash: block.transactions[0].hash,
            log_idx: U64::from(0),
            address: *Address::with_last_byte(0x66),
            topics: vec![erc20_transfer_topic(), from_topic, to_topic],
            data: Bytes::from(amount),
        }
    }

    #[tokio::test]
    async fn copy_txs_preserves_contract_creation_null_to() {
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        let mut pg = pool.get().await.expect("test pg client");
        let pgtx = pg.transaction().await.expect("test transaction");
        let block = test_block();
        setup_tables(&pgtx, 1, block.number.to(), None)
            .await
            .expect("setting up tables");
        copy_txs(&pgtx, api::Chain(1), &[block])
            .await
            .expect("copying txs");
        pgtx.commit().await.expect("committing test transaction");

        let row = pool
            .get()
            .await
            .expect("test pg client")
            .query_one(
                "select \"to\", max_fee_per_gas::text, max_priority_fee_per_gas::text from txs where chain = 1 and block_num = 1",
                &[],
            )
            .await
            .expect("querying tx");
        let to: Option<Vec<u8>> = row.get(0);
        let max_fee_per_gas: Option<String> = row.get(1);
        let max_priority_fee_per_gas: Option<String> = row.get(2);
        assert_eq!(to, None);
        assert_eq!(max_fee_per_gas.as_deref(), Some("60"));
        assert_eq!(max_priority_fee_per_gas.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn copy_blocks_persists_parent_hash() {
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        let mut pg = pool.get().await.expect("test pg client");
        let pgtx = pg.transaction().await.expect("test transaction");
        let block = test_block();
        setup_tables(&pgtx, 1, block.number.to(), None)
            .await
            .expect("setting up tables");
        copy_blocks(&pgtx, api::Chain(1), &[block])
            .await
            .expect("copying blocks");
        pgtx.commit().await.expect("committing test transaction");

        let row = pool
            .get()
            .await
            .expect("test pg client")
            .query_one(
                "select parent_hash, base_fee_per_gas::text from blocks where chain = 1 and num = 1",
                &[],
            )
            .await
            .expect("querying block");
        let parent_hash: Vec<u8> = row.get(0);
        let base_fee_per_gas: Option<String> = row.get(1);
        assert_eq!(parent_hash, B256::with_last_byte(0x0f).to_vec());
        assert_eq!(base_fee_per_gas.as_deref(), Some("40"));
    }

    #[tokio::test]
    async fn copy_receipts_persists_mvp_receipt_fields() {
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        let mut pg = pool.get().await.expect("test pg client");
        let pgtx = pg.transaction().await.expect("test transaction");
        let block = test_block();
        let receipt = test_receipt(&block);
        setup_tables(&pgtx, 1, block.number.to(), None)
            .await
            .expect("setting up tables");
        copy_receipts(&pgtx, api::Chain(1), &[receipt])
            .await
            .expect("copying receipts");
        pgtx.commit().await.expect("committing test transaction");

        let row = pool
            .get()
            .await
            .expect("test pg client")
            .query_one(
                "select status, gas_used, cumulative_gas_used, effective_gas_price, contract_address from receipts where chain = 1 and tx_hash = $1",
                &[&block.transactions[0].hash],
            )
            .await
            .expect("querying receipt");
        let status: Option<bool> = row.get(0);
        let gas_used: String = row.get(1);
        let cumulative_gas_used: String = row.get(2);
        let effective_gas_price: Option<String> = row.get(3);
        let contract_address: Option<Vec<u8>> = row.get(4);
        assert_eq!(status, Some(true));
        assert_eq!(gas_used, "21000");
        assert_eq!(cumulative_gas_used, "21000");
        assert_eq!(effective_gas_price.as_deref(), Some("41"));
        assert_eq!(
            contract_address,
            Some(Address::with_last_byte(0x33).to_vec())
        );
    }

    #[tokio::test]
    async fn copy_erc20_transfers_from_logs_extracts_transfer_rows() {
        let pool = shared::pg::test::new(SCHEMA_BE).await;
        let mut pg = pool.get().await.expect("test pg client");
        let pgtx = pg.transaction().await.expect("test transaction");
        let block = test_block();
        let log = test_transfer_log(&block);
        setup_tables(&pgtx, 1, block.number.to(), None)
            .await
            .expect("setting up tables");
        copy_logs(&pgtx, api::Chain(1), vec![log])
            .await
            .expect("copying logs");
        copy_erc20_transfers_from_logs(&pgtx, api::Chain(1), 1, 1)
            .await
            .expect("copying erc20 transfers");
        pgtx.commit().await.expect("committing test transaction");

        let row = pool
            .get()
            .await
            .expect("test pg client")
            .query_one(
                r#"
                select
                    encode(token_address, 'hex') as token_address,
                    encode("from", 'hex') as from_address,
                    encode("to", 'hex') as to_address,
                    amount::text as amount
                from erc20_transfers
                where chain = 1 and block_num = 1 and log_idx = 0
                "#,
                &[],
            )
            .await
            .expect("querying erc20 transfer");
        let token_address: String = row.get("token_address");
        let from_address: String = row.get("from_address");
        let to_address: String = row.get("to_address");
        let amount: String = row.get("amount");
        assert_eq!(token_address, hex::encode(Address::with_last_byte(0x66)));
        assert_eq!(from_address, hex::encode(Address::with_last_byte(0x44)));
        assert_eq!(to_address, hex::encode(Address::with_last_byte(0x55)));
        assert_eq!(amount, "123");
    }
}
