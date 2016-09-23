SELECT
  symbol,
  MIN(low) AS lowest_price,
  MAX(high) AS highest_price,
  AVG(volume) AS avg_volume,
  COUNT(1) AS count
FROM nasdaq
GROUP BY symbol
ORDER BY symbol
