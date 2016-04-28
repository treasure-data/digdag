SELECT count(*)
FROM sample_datasets.www_access
WHERE TD_TIME_RANGE(time,
    TD_TIME_ADD('${session_date}', '-${from_days_ago}d'),
    TD_TIME_ADD('${session_date}', '-${to_days_ago}d')
);
