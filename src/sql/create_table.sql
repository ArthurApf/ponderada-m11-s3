CREATE TABLE IF NOT EXISTS working_data (
    data_ingestao DateTime,
    number UInt32,
    name String,
    type String,
    weight UInt32,
    tag String
) ENGINE = MergeTree()
ORDER BY data_ingestao;
