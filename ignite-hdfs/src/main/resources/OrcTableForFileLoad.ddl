CREATE TABLE IF NOT EXISTS TEST_FROM_FILESYSTEM (
MSISDN INT,
CAMPAIGN_ID INT,
SMS_TYPE STRING,
STARTTIME TIMESTAMP,
SUBS_KEY BIGINT,
SEND_DT TIMESTAMP,
DELIVERY_DT TIMESTAMP,
START_SEND_DT TIMESTAMP,
STATUS_ID INT,
RESULT_ID BIGINT,
ERROR_TEXT STRING,
MAX_TIME_OFFSET BIGINT
)
STORED AS ORC