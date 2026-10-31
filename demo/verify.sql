SELECT 
    figi, 
    direction, 
    price_rub, 
    quantity, 
    trade_datetime 
FROM dds.dds_trades 
ORDER BY loaded_at DESC 
LIMIT 5;