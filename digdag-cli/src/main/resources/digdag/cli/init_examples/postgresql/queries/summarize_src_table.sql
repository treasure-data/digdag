SELECT
  code,
  method,
  MAX(LENGTH(agent)) AS longest_ua,
  COUNT(*) AS count
FROM example_access_logs
GROUP BY code, method
ORDER BY code, method

