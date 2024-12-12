case class StockMetrics(
                         symbol: String,
                         date: String,
                         `price:daily_open`: Double,      // matches price:daily_open#b
                         `price:daily_close`: Double,     // matches price:daily_close#b
                         `price:daily_change`: Double,    // matches price:daily_change#b
                         `price:return_pct`: Double,      // matches price:return_pct#b
                         `price:trading_range`: Double,   // matches price:trading_range#b
                         `price:volatility`: Double,      // matches price:volatility#b
                         `volume:daily_volume`: Long,     // matches volume:daily_volume#b
                         `volume:price_volume`: Double,   // matches volume:price_volume#b
                         `trends:ma5`: Double,           // matches trends:ma5#b
                         `trends:ma10`: Double,          // matches trends:ma10#b
                         `trends:ma20`: Double,          // matches trends:ma20#b
                         `trends:ma50`: Double           // matches trends:ma50#b
                       )