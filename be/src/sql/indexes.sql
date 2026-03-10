create index if not exists logs_address_topic_idx on logs((topics[1]), address, block_num desc);
create index if not exists logs_topics_2 on logs((topics[2]));
create index if not exists logs_topics_3 on logs((topics[3]));
create index if not exists logs_topics_4 on logs((topics[4]));
create index if not exists logs_block_num on logs(block_num);
create index if not exists logs_tx_hash on logs(chain, tx_hash);


create index if not exists txs_block on txs(block_num);
create index if not exists txs_hash on txs(hash);
create index if not exists txs_from on txs("from");
create index if not exists txs_to on txs("to");
create index if not exists txs_calls on txs using gin (calls);

create index if not exists txs_selector
on txs (substring(input, 1, 4))
where input is not null and octet_length(input) >= 4;


create unique index if not exists blocks_chain_num on blocks(chain, num);
create index if not exists blocks_timestamp on blocks(timestamp);
create index if not exists blocks_hash on blocks(chain, hash);

create index if not exists receipts_chain_block_num_tx_hash on receipts(chain, block_num, tx_hash);
create index if not exists receipts_chain_tx_hash on receipts(chain, tx_hash);
create index if not exists receipts_chain_block_num on receipts(chain, block_num);

create unique index if not exists erc20_transfers_chain_tx_log_idx
on erc20_transfers(chain, tx_hash, log_idx);

create index if not exists erc20_transfers_chain_token_block
on erc20_transfers(chain, token_address, block_num desc);

create index if not exists erc20_transfers_chain_from_token
on erc20_transfers(chain, "from", token_address);

create index if not exists erc20_transfers_chain_to_token
on erc20_transfers(chain, "to", token_address);
