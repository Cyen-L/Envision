-- Create database if not exists
CREATE DATABASE IF NOT EXISTS olap_db;

-- Raw transactions table
CREATE TABLE IF NOT EXISTS olap_db.transactions (
    transaction_id    UInt64,
    bill_id           UInt64,
    site_id           UInt32,
    username          String,
    item_id           UInt32,
    currency_code     String,
    transfer_amount   Decimal(18, 2),
    transaction_time  DateTime64(3), 
    _version          UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (site_id, username, transaction_time, _version)
PARTITION BY toYYYYMM(transaction_time);