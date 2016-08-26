CREATE EXTERNAL TABLE IF NOT EXISTS TEMP_TABLE
                 (
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location ?