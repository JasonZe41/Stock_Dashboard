create 'stock_metrics', 'price', 'volume', 'trends'

CREATE EXTERNAL TABLE stock_metrics (
    stock_key string,
    -- Daily price metrics
    daily_open double,
    daily_close double,
    daily_price_change double,
    daily_return_percentage double,
    daily_trading_range double,
    daily_volatility double,
    
    -- Volume metrics
    daily_volume bigint,
    price_volume double,
    
    -- Moving averages
    ma5 double,
    ma10 double,
    ma20 double,
    ma50 double
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = 
    ':key,
    price:daily_open#b,
    price:daily_close#b,
    price:daily_change#b,
    price:return_pct#b,
    price:trading_range#b,
    price:volatility#b,
    volume:daily_volume#b,
    volume:price_volume#b,
    trends:ma5#b,
    trends:ma10#b,
    trends:ma20#b,
    trends:ma50#b'
)
TBLPROPERTIES ('hbase.table.name' = 'stock_metrics');


INSERT OVERWRITE TABLE stock_metrics 
SELECT 
    concat(symbol, ':', `date`) as stock_key,
    open as daily_open,
    close as daily_close,
    (close - open) as daily_price_change,
    ((close - open)/open * 100) as daily_return_percentage,
    (high - low) as daily_trading_range,
    ((high - low)/open) as daily_volatility,
    volume as daily_volume,
    (close * volume) as price_volume,
    close as ma5,    -- placeholder
    close as ma10,   -- placeholder
    close as ma20,   -- placeholder
    close as ma50    -- placeholder
FROM yanze41_stock_data;