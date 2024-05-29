use role accountadmin;
show roles like '%_RW%';

-- for role setup if needed --
use role securityadmin;
create role if not exists user_dbt_test comment='USER ROLE: A role created for DBT test. This write/create/update for test env ONLY role inherits all dm_ladders_test_rw';

create role if not exists dm_ladders_test_rw comment='Read/Write/Delete/Create role allowing DML operations to all tables and views in the dm_ladders_test database. Users need to have database "USAGE" grants before dm_ladders_test_rw will take effect.';

//create role if not exists procedures_test_rw comment='Read/Write/Delete/Create role allowing DML operations to procedures in the METADATA.PROCEDURES_TEST schema. Users need to have database "USAGE" grants before procedures_test_rw will take effect.';

grant role dm_ladders_test_rw to role user_dbt_test;
grant role dm_ladders_test_rw to role sysadmin;

// for the purpose of accessing HADES_PROD_BHA_DIMAGI_SHARE
grant role USER_DIMAGI_ANALYST to role dm_ladders_test_rw;

//grant role procedures_test_rw to role user_dbt_test;
//grant role procedures_test_rw to role sysadmin;

// below is important and needed
grant role USER_DBT_TEST to role sysadmin;

grant role user_dbt_test to user SVC_DBT_TEST;
grant role user_dbt_test to user slu;

use role accountadmin;
grant role dm_ladders_dev_usage to role user_dimagi_analyst;
grant role dm_ladders_dev_dm_r to role user_dimagi_analyst;
grant role dm_ladders_dev_util_r to role user_dimagi_analyst;

grant role dm_ladders_prod_usage to role user_dimagi_analyst;
grant role dm_ladders_prod_dm_r to role user_dimagi_analyst;
grant role dm_ladders_prod_util_r to role user_dimagi_analyst;

grant role dm_ladders_dev_usage to role user_tableau_oauth_bha;
grant role dm_ladders_dev_dm_r to role user_tableau_oauth_bha;
grant role dm_ladders_dev_util_r to role user_tableau_oauth_bha;
-- end for role setup if needed --


-- for exising replicated db --
use role accountadmin;
-- db
grant usage on database dm_co_care_coord_test to role DM_CO_CARE_COORD_DEV_USAGE;
// for testing project
grant usage on database dm_co_care_coord_test to role dm_ladders_test_rw;

-- schema DL
grant usage on schema dm_co_care_coord_test.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant select on future tables in schema dm_co_care_coord_test.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant select on future views in schema dm_co_care_coord_test.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant select on all tables in schema dm_co_care_coord_test.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant select on all views in schema dm_co_care_coord_test.DL to role DM_CO_CARE_COORD_DEV_DL_R;
// for testing project
grant usage on schema dm_co_care_coord_test.DL to role dm_ladders_test_rw;
grant select on future tables in schema dm_co_care_coord_test.DL to role dm_ladders_test_rw;
grant select on future views in schema dm_co_care_coord_test.DL to role dm_ladders_test_rw;
grant select on all tables in schema dm_co_care_coord_test.DL to role dm_ladders_test_rw;
grant select on all views in schema dm_co_care_coord_test.DL to role dm_ladders_test_rw;

-- schema DM
grant usage on schema dm_co_care_coord_test.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on future tables in schema dm_co_care_coord_test.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on future views in schema dm_co_care_coord_test.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on all tables in schema dm_co_care_coord_test.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on all views in schema dm_co_care_coord_test.DM to role DM_CO_CARE_COORD_DEV_DM_R;
// for testing project
grant usage on schema dm_co_care_coord_test.DM to role dm_ladders_test_rw;
grant select on future tables in schema dm_co_care_coord_test.DM to role dm_ladders_test_rw;
grant select on future views in schema dm_co_care_coord_test.DM to role dm_ladders_test_rw;
grant select on all tables in schema dm_co_care_coord_test.DM to role dm_ladders_test_rw;
grant select on all views in schema dm_co_care_coord_test.DM to role dm_ladders_test_rw;

-- schema INTEGRATION
grant usage on schema dm_co_care_coord_test.integration to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant select on future tables in schema dm_co_care_coord_test.integration to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant select on future views in schema dm_co_care_coord_test.integration to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant select on all tables in schema dm_co_care_coord_test.integration to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant select on all views in schema dm_co_care_coord_test.integration to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
// for testing project
grant usage on schema dm_co_care_coord_test.integration to role dm_ladders_test_rw;
grant select on future tables in schema dm_co_care_coord_test.integration to role dm_ladders_test_rw;
grant select on future views in schema dm_co_care_coord_test.integration to role dm_ladders_test_rw;
grant select on all tables in schema dm_co_care_coord_test.integration to role dm_ladders_test_rw;
grant select on all views in schema dm_co_care_coord_test.integration to role dm_ladders_test_rw;

-- schema UTIL
grant usage on schema dm_co_care_coord_test.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant select on future tables in schema dm_co_care_coord_test.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant select on future views in schema dm_co_care_coord_test.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant select on all tables in schema dm_co_care_coord_test.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant select on all views in schema dm_co_care_coord_test.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
// for testing project
grant usage on schema dm_co_care_coord_test.UTIL to role dm_ladders_test_rw;
grant select on future tables in schema dm_co_care_coord_test.UTIL to role dm_ladders_test_rw;
grant select on future views in schema dm_co_care_coord_test.UTIL to role dm_ladders_test_rw;
grant select on all tables in schema dm_co_care_coord_test.UTIL to role dm_ladders_test_rw;
grant select on all views in schema dm_co_care_coord_test.UTIL to role dm_ladders_test_rw;
-- end for exising replicated db --


-- for ladders db --
use role accountadmin;
// for read access
grant usage on database dm_ladders_test to role dm_ladders_dev_usage;
grant usage on schema dm_ladders_test.dm to role dm_ladders_dev_dm_r;
grant usage on schema dm_ladders_test.util to role dm_ladders_dev_util_r;
grant usage on future functions in schema dm_ladders_test.dm to role dm_ladders_dev_dm_r;
grant select on all tables in schema dm_ladders_test.dm to role dm_ladders_dev_dm_r;
grant select on future tables in schema dm_ladders_test.dm to role dm_ladders_dev_dm_r;
grant select on all views in schema dm_ladders_test.dm to role dm_ladders_dev_dm_r;
grant select on future views in schema dm_ladders_test.dm to role dm_ladders_dev_dm_r;
grant select on all tables in schema dm_ladders_test.util to role dm_ladders_dev_util_r;
grant select on future tables in schema dm_ladders_test.util to role dm_ladders_dev_util_r;
grant select on all views in schema dm_ladders_test.util to role dm_ladders_dev_util_r;
grant select on future views in schema dm_ladders_test.util to role dm_ladders_dev_util_r;

-- for testing project - for write access
use role accountadmin;
// for database
grant usage on database dm_ladders_test to role dm_ladders_test_rw;

-- for schema DM
grant usage on schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant create function on schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant create stage on schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant create task on schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant create file format on schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant create table on schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant create view on schema dm_ladders_test.DM to role dm_ladders_test_rw;
-- delete
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all tables in schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all views in schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema dm_ladders_test.DM to role dm_ladders_test_rw;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema dm_ladders_test.DM to role dm_ladders_test_rw;

-- for schema UTIL
grant usage on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant create function on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant create stage on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant create task on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant create file format on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant create table on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant create view on schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
-- delete
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all tables in schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all views in schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema dm_ladders_test.UTIL to role dm_ladders_test_rw;
-- end for testing project - for write access



// stored procedures
//grant usage on schema METADATA.procedures_test to role procedures_test_rw;
//grant create procedure on schema METADATA.procedures_test to role procedures_test_rw;
//grant create view on schema METADATA.procedures_test to role procedures_test_rw;

/*
CREATE FUNCTION
CREATE TABLE
CREATE VIEW
DELETE - FUTURE TABLE
DELETE - FUTURE VIEW
INSERT - FUTURE TABLE
INSERT - FUTURE VIEW
SELECT - FUTURE TABLE
SELECT - FUTURE VIEW
TRUNCATE - FUTURE TABLE
TRUNCATE - FUTURE VIEW
UPDATE - FUTURE TABLE
UPDATE - FUTURE VIEW
USAGE

*/


