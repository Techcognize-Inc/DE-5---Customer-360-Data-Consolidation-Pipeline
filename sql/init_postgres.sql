CREATE TABLE IF NOT EXISTS watermark_config (
    source_name VARCHAR(50) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP NOT NULL
);

INSERT INTO watermark_config (source_name, last_processed_timestamp)
VALUES
    ('cdc.customers', '1970-01-01 00:00:00'),
    ('cdc.accounts', '1970-01-01 00:00:00'),
    ('cdc.loans', '1970-01-01 00:00:00')
ON CONFLICT (source_name) DO NOTHING;