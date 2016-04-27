SELECT count(*)
FROM sample_datasets.www_access
WHERE TD_TIME_RANGE(time,
    TD_TIME_ADD(TD_DATE_TRUNC('day', TD_SCHEDULED_TIME()), '-${from_days_ago}d'),
    TD_TIME_ADD(TD_DATE_TRUNC('day', TD_SCHEDULED_TIME()), '-${to_days_ago}d')
);
