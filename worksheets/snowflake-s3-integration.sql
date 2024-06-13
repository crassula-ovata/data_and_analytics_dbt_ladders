use role accountadmin;
// the following can only be created by accountadmin
create or replace storage integration s3_int_obj  
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::936263849005:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = (
      's3://commcare-snowflake-data-sync/co-carecoordination-dev/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-uat/', 
      's3://commcare-snowflake-data-sync/co-carecoordination/', 
      's3://commcare-snowflake-data-sync/navajo-covid19-staging/', 
      's3://commcare-snowflake-data-sync/hw-covid-response-team/')
   COMMENT = 'storage integration object for aws s3';


alter storage integration s3_int_obj set STORAGE_ALLOWED_LOCATIONS = (
      's3://commcare-snowflake-data-sync/bha-location-redesign-1/', 
      's3://commcare-snowflake-data-sync/staging-co-carecoordination-test/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-test/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-perf/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-dev/', 
      's3://commcare-snowflake-data-sync/co-carecoordination-uat/', 
      's3://commcare-snowflake-data-sync/co-carecoordination/', 
      's3://commcare-snowflake-data-sync/navajo-covid19-staging/', 
      's3://commcare-snowflake-data-sync/chw-covid-response-team/');

DESC integration s3_int_obj;

grant usage on integration s3_int_obj to role sysadmin;