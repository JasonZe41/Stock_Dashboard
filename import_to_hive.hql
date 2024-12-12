



DELETE JARS;

ADD JAR wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/tmp/yanze41/uber-stock-1.0-SNAPSHOT.jar;

LIST JARS;

DROP TABLE IF EXISTS yanze41_stock_data;

CREATE EXTERNAL TABLE yanze41_stock_data (
    symbol STRING,
    market STRING,
    trade_date STRING,
    low DOUBLE,
    open DOUBLE,
    volume BIGINT,
    high DOUBLE,
    close DOUBLE,
    adjustedclose DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
WITH SERDEPROPERTIES (
    'serialization.class'='org.example.StockData',
    'serialization.format'='org.apache.thrift.protocol.TBinaryProtocol'
)
STORED AS SEQUENCEFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/yanze41/stockdata-output';


