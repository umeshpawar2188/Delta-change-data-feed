CREATE TABLE IF NOT EXISTS lseg_lh_silver_ns.etl_metadata (
    table_name STRING,
    last_processed_version INT,
    last_processed_timestamp TIMESTAMP
);

INSERT INTO lseg_lh_silver_ns.etl_metadata (table_name, last_processed_version, last_processed_timestamp)
VALUES ('Silver_StockTrades', 0, current_timestamp());