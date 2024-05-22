// email notification object
use role accountadmin;
//drop INTEGRATION email_int_obj;
CREATE OR REPLACE NOTIFICATION INTEGRATION email_int_obj
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('slu@dimagi.com','rsingh@dimagi.com', 'ush-devops@dimagi.com');

DESC integration email_int_obj;

//GRANT USAGE ON INTEGRATION email_int_obj TO ROLE sysadmin;


// DM_LADDERS_DEV

use role accountadmin;
use database DM_LADDERS_DEV;
use schema UTIL;

CREATE OR REPLACE ALERT DM_LADDERS_DEV.UTIL.s3_int_alert
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 00 06 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_DEV.util.sql_logs where execution_status <> 'SUCCESS' and DATE(start_time) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com, ush-devops@dimagi.com',
    'Email Alert: DM_LADDERS_DEV sql_logs error.',
    'there are errors from util.sql_logs\n start_time='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

CREATE OR REPLACE ALERT DM_LADDERS_DEV.UTIL.S3_TASK_ALERT
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 05 06 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_DEV.util.task_log where status <> 'SUCCESS' and DATE(task_start) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com, ush-devops@dimagi.com',
    'Email Alert: DM_LADDERS_DEV task_log error.',
    'there are errors from util.task_log\n task_start='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DM_LADDERS_DEV.UTIL.s3_int_alert;
alter alert DM_LADDERS_DEV.UTIL.S3_TASK_ALERT resume;
alter alert DM_LADDERS_DEV.UTIL.S3_TASK_ALERT suspend;

show alerts; 

// DM_LADDERS_QA

use role accountadmin;
use database DM_LADDERS_QA;
use schema UTIL;

CREATE OR REPLACE ALERT DM_LADDERS_QA.UTIL.s3_int_alert
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 00 07 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_QA.util.sql_logs where execution_status <> 'SUCCESS' and DATE(start_time) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com, ush-devops@dimagi.com',
    'Email Alert: DM_LADDERS_QA sql_logs error.',
    'there are errors from util.sql_logs\n start_time='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

CREATE OR REPLACE ALERT DM_LADDERS_QA.UTIL.S3_TASK_ALERT
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 05 07 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_QA.util.task_log where status <> 'SUCCESS' and DATE(task_start) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com, ush-devops@dimagi.com',
    'Email Alert: DM_LADDERS_QA task_log error.',
    'there are errors from util.task_log\n task_start='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DM_LADDERS_QA.UTIL.S3_TASK_ALERT;
alter alert DM_LADDERS_QA.UTIL.S3_TASK_ALERT resume;
alter alert DM_LADDERS_QA.UTIL.S3_TASK_ALERT suspend;

show alerts; 

// DM_LADDERS_PROD

use role accountadmin;
use database DM_LADDERS_PROD;
use schema UTIL;

CREATE OR REPLACE ALERT DM_LADDERS_PROD.UTIL.s3_int_alert
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 00 07 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_PROD.util.sql_logs where execution_status <> 'SUCCESS' and DATE(start_time) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com, ush-devops@dimagi.com',
    'Email Alert: DM_LADDERS_PROD sql_logs error.',
    'there are errors from util.sql_logs\n start_time='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

CREATE OR REPLACE ALERT DM_LADDERS_PROD.UTIL.S3_TASK_ALERT
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 05 07 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_PROD.util.task_log where status <> 'SUCCESS' and DATE(task_start) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com, ush-devops@dimagi.com',
    'Email Alert: DM_LADDERS_PROD task_log error.',
    'there are errors from util.task_log\n task_start='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DM_LADDERS_PROD.UTIL.S3_TASK_ALERT;
alter alert DM_LADDERS_PROD.UTIL.S3_TASK_ALERT resume;
alter alert DM_LADDERS_PROD.UTIL.S3_TASK_ALERT suspend;

show alerts; 


// DM_LADDERS_TEST_STAGING

use role accountadmin;
use database DM_LADDERS_TEST_STAGING;
use schema UTIL;

CREATE OR REPLACE ALERT DM_LADDERS_TEST_STAGING.UTIL.S3_TASK_ALERT
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 25 06 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_TEST_STAGING.util.task_log where status <> 'SUCCESS' and DATE(task_start) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com',
    'Email Alert: DM_LADDERS_TEST_STAGING task_log error.',
    'there are errors from util.task_log\n task_start='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DM_LADDERS_TEST_STAGING.UTIL.s3_int_alert;
alter alert DM_LADDERS_TEST_STAGING.UTIL.S3_TASK_ALERT resume;
alter alert DM_LADDERS_TEST_STAGING.UTIL.S3_TASK_ALERT suspend;

show alerts; 


// DM_LADDERS_PERF

use role accountadmin;
use database DM_LADDERS_PERF;
use schema UTIL;

CREATE OR REPLACE ALERT DM_LADDERS_PERF.UTIL.S3_TASK_ALERT
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 35 06 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_PERF.util.task_log where status <> 'SUCCESS' and DATE(task_start) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com',
    'Email Alert: DM_LADDERS_PERF task_log error.',
    'there are errors from util.task_log\n task_start='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DM_LADDERS_PERF.UTIL.s3_int_alert;
alter alert DM_LADDERS_PERF.UTIL.S3_TASK_ALERT resume;
alter alert DM_LADDERS_PERF.UTIL.S3_TASK_ALERT suspend;

show alerts; 


// DM_LADDERS_TEST

use role accountadmin;
use database DM_LADDERS_TEST;
use schema UTIL;

CREATE OR REPLACE ALERT DM_LADDERS_TEST.UTIL.S3_TASK_ALERT
WAREHOUSE = COMPUTE_WH
schedule='USING CRON 35 05 * * * America/New_York'
  IF (EXISTS(
    select * from DM_LADDERS_TEST.util.task_log where status <> 'SUCCESS' and DATE(task_start) > DATEADD(days, -1, sysdate())
    ))
  THEN
  CALL SYSTEM$SEND_EMAIL(
    'email_int_obj',
    'slu@dimagi.com',
    'Email Alert: DM_LADDERS_TEST task_log error.',
    'there are errors from util.task_log\n task_start='||DATE(CURRENT_DATE)||' greater than '||DATEADD(days, -1, CURRENT_DATE)
);

use role accountadmin;
//drop alert DM_LADDERS_TEST.UTIL.s3_int_alert;
alter alert DM_LADDERS_TEST.UTIL.S3_TASK_ALERT resume;
alter alert DM_LADDERS_TEST.UTIL.S3_TASK_ALERT suspend;

show alerts; 



