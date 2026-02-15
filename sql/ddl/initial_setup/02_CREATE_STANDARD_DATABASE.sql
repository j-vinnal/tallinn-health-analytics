/*
# To run this file with Snowsql:
# 0. If not done already then define the snowflake connection in file: PowerShell.exe ~\.snowsql\config
# * You can add what ever account where you have at least SYSADMIN and SECURITYADMIN roles available for the user:
# [connections.strat]
# accountname = "sf29869.north-europe.azure"
# username = "STRATUSERADMIN"
# password = 
# 1. cd to the git project folder. The default should be then:
# * On Windows CMD>cd %userprofile%\git\repos\strat\
# * On Windows PowerShell> cd ~\git\repos\strat\
# 2. Make the connection:
snowsql -c strat
# 3. Run the files
# !load .\sql\ddl\initial_setup\01_SET_ACCOUNT_LEVEL_ATTRIBUTES_CREATE_USER_WITH_SYSADMIN_SECURITYADMIN_ROLE.sql
# !load .\sql\ddl\initial_setup\02_CREATE_STANDARD_DATABASE.sql
# !load .\sql\ddl\initial_setup\03_TECHNICAL.sql
# Or you can load those by hand:
# NB! To close snowsql connection use:
!quit
*/

-----------------------------------------------------------------------------------------SET SCRIPT VARIABLES
--ENVIRONMENT
SET var_account_id = 'STRAT'; --customer identifier, for example STRAT for strat
SET var_environment_id = 'DEV'; --environment identifier, DEV, TEST, PROD, LIVE
SET var_account_environment_id=$var_account_id||'_'||$var_environment_id; --STRAT_DEV

-----------------------------------------------------------------------------------------DATABASE
USE ROLE SYSADMIN;

--DATABASE
SET var_database_id=$var_account_environment_id||'_DB'; --STRAT_DEV_DB
SET var_database_id_comment=$var_account_id||' '||$var_environment_id||' database';
CREATE DATABASE IF NOT EXISTS identifier($var_database_id) COMMENT = $var_database_id_comment;

USE DATABASE identifier($var_database_id);

CREATE SCHEMA IF NOT EXISTS DWH;
CREATE SCHEMA IF NOT EXISTS ODS;
CREATE SCHEMA IF NOT EXISTS TECHNICAL;
CREATE SCHEMA IF NOT EXISTS MANUAL;
CREATE FILE FORMAT IF NOT EXISTS MANUAL.file_format_csv_semicolondelimiter
  type = 'CSV'
  field_delimiter = ';'
  skip_header = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ;

CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE STAGE STAGING.MY_STAGE;
CREATE FILE FORMAT IF NOT EXISTS STAGING.file_format_csv_pipedelimiter_doublequote_enclosure
  type = 'CSV'
  field_delimiter = '|'
  skip_header = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ;

CREATE FILE FORMAT IF NOT EXISTS STAGING.file_format_csv_comma_doublequote_enclosure
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-----------------------------------------------------------------------------------------WAREHOUSES
USE ROLE SYSADMIN;

SET var_warehouse_id_etl=$var_account_environment_id||'_WH_ETL';
CREATE WAREHOUSE IF NOT EXISTS identifier($var_warehouse_id_etl) WITH
  WAREHOUSE_SIZE='X-SMALL'
  AUTO_SUSPEND = 60 -- seconds
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED=TRUE;

SET var_warehouse_id_analytics=$var_account_environment_id||'_WH_ANALYTICS';
CREATE WAREHOUSE IF NOT EXISTS identifier($var_warehouse_id_analytics) WITH
  WAREHOUSE_SIZE='X-SMALL'
  AUTO_SUSPEND = 60 -- seconds
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED=TRUE;

-----------------------------------------------------------------------------------------USERS, ROLES & GRANTS
USE ROLE SYSADMIN;

GRANT USAGE ON DATABASE identifier($var_database_id) TO SECURITYADMIN;

-----------------------------------------------------------------------------------------USER/ROLE ETL
USE ROLE SECURITYADMIN;

USE DATABASE identifier($var_database_id);

SET var_role_id_etl=$var_account_environment_id||'_ROLE_ETL';
SET var_user_id_etl=$var_account_environment_id||'_USER_ETL';
CREATE ROLE IF NOT EXISTS identifier($var_role_id_etl) COMMENT = 'Role for doing ETL';
CREATE USER IF NOT EXISTS identifier($var_user_id_etl)
  LOGIN_NAME = $var_user_id_etl
  DISPLAY_NAME = $var_user_id_etl
  PASSWORD = 'Stratusertallinn35'
  MUST_CHANGE_PASSWORD = FALSE
  DEFAULT_WAREHOUSE = $var_warehouse_id_etl
  DEFAULT_NAMESPACE = $var_database_id
  DEFAULT_ROLE = $var_role_id_etl
  COMMENT = 'User for doing ETL';
GRANT ROLE identifier($var_role_id_etl) TO USER identifier($var_user_id_etl);

--DB
GRANT USAGE ON WAREHOUSE identifier($var_warehouse_id_etl) TO ROLE identifier($var_role_id_etl);

--WH
GRANT USAGE ON DATABASE identifier($var_database_id) TO identifier($var_role_id_etl);

--DWH
GRANT USAGE ON SCHEMA DWH TO identifier($var_role_id_etl);
GRANT SELECT,DELETE,INSERT,UPDATE ON ALL TABLES IN SCHEMA DWH TO ROLE identifier($var_role_id_etl);
GRANT SELECT,DELETE,INSERT,UPDATE ON FUTURE TABLES IN SCHEMA DWH TO ROLE identifier($var_role_id_etl);
GRANT SELECT ON ALL VIEWS IN SCHEMA DWH TO ROLE identifier($var_role_id_etl);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DWH TO ROLE identifier($var_role_id_etl);

--ODS
GRANT USAGE ON SCHEMA ODS TO identifier($var_role_id_etl);
GRANT SELECT,DELETE,INSERT,UPDATE ON ALL TABLES IN SCHEMA ODS TO ROLE identifier($var_role_id_etl);
GRANT SELECT,DELETE,INSERT,UPDATE ON FUTURE TABLES IN SCHEMA ODS TO ROLE identifier($var_role_id_etl);

--STAGING
GRANT ALL ON SCHEMA STAGING TO identifier($var_role_id_etl);
GRANT ALL ON ALL TABLES IN SCHEMA STAGING TO ROLE identifier($var_role_id_etl);
GRANT ALL ON FUTURE TABLES IN SCHEMA STAGING TO ROLE identifier($var_role_id_etl);

GRANT READ,WRITE ON STAGE STAGING.MY_STAGE TO ROLE identifier($var_role_id_etl);

GRANT USAGE ON FILE FORMAT STAGING.file_format_csv_pipedelimiter_doublequote_enclosure TO ROLE identifier($var_role_id_etl);
GRANT USAGE ON FILE FORMAT MANUAL.file_format_csv_semicolondelimiter TO ROLE identifier($var_role_id_etl);
GRANT USAGE ON FILE FORMAT STAGING.file_format_csv_comma_doublequote_enclosure TO ROLE identifier($var_role_id_etl);

--TECHNICAL
GRANT USAGE ON SCHEMA TECHNICAL TO identifier($var_role_id_etl);
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA TECHNICAL TO ROLE identifier($var_role_id_etl);
GRANT SELECT,INSERT,UPDATE,DELETE ON FUTURE TABLES IN SCHEMA TECHNICAL TO ROLE identifier($var_role_id_etl);
GRANT USAGE ON ALL SEQUENCES IN SCHEMA TECHNICAL TO ROLE identifier($var_role_id_etl);
GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA TECHNICAL TO ROLE identifier($var_role_id_etl);

-----------------------------------------------------------------------------------------USER/ROLE ANALYTICS
USE ROLE SECURITYADMIN;

USE DATABASE identifier($var_database_id);
  
SET var_role_id_analytics=$var_account_environment_id||'_ROLE_ANALYTICS';
SET var_user_id_analytics=$var_account_environment_id||'_USER_ANALYTICS';
CREATE ROLE IF NOT EXISTS identifier($var_role_id_analytics) COMMENT = 'Role for accessing data from analytics';
CREATE USER IF NOT EXISTS identifier($var_user_id_analytics)
  LOGIN_NAME = $var_user_id_analytics
  DISPLAY_NAME = $var_user_id_analytics
  PASSWORD = 'Stratusertallinn35'
  MUST_CHANGE_PASSWORD = FALSE
  DEFAULT_WAREHOUSE = $var_warehouse_id_analytics
  DEFAULT_NAMESPACE = $var_database_id
  DEFAULT_ROLE = $var_role_id_analytics
  COMMENT = 'Role for accessing data from analytics';
GRANT ROLE identifier($var_role_id_analytics) TO USER identifier($var_user_id_analytics);

--DB
GRANT USAGE ON DATABASE identifier($var_database_id) TO identifier($var_role_id_analytics);

--WH
GRANT USAGE ON WAREHOUSE identifier($var_warehouse_id_analytics) TO ROLE identifier($var_role_id_analytics);

--DWH
GRANT USAGE ON SCHEMA DWH TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON ALL TABLES IN SCHEMA DWH TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON ALL VIEWS IN SCHEMA DWH TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON FUTURE TABLES IN SCHEMA DWH TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DWH TO ROLE identifier($var_role_id_analytics);

--ODS
GRANT USAGE ON SCHEMA ODS TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON ALL TABLES IN SCHEMA ODS TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON FUTURE TABLES IN SCHEMA ODS TO ROLE identifier($var_role_id_analytics);

--TECHNICAL
GRANT USAGE ON SCHEMA TECHNICAL TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON ALL TABLES IN SCHEMA TECHNICAL TO ROLE identifier($var_role_id_analytics);
GRANT SELECT ON FUTURE TABLES IN SCHEMA TECHNICAL TO ROLE identifier($var_role_id_analytics);

--MANUAL
GRANT ALL ON SCHEMA MANUAL TO identifier($var_role_id_analytics);
GRANT ALL ON ALL TABLES IN SCHEMA MANUAL TO ROLE identifier($var_role_id_analytics);
GRANT ALL ON FUTURE TABLES IN SCHEMA MANUAL TO ROLE identifier($var_role_id_analytics);
GRANT USAGE ON FILE FORMAT MANUAL.file_format_csv_semicolondelimiter TO ROLE identifier($var_role_id_analytics);
GRANT USAGE ON FILE FORMAT STAGING.file_format_csv_pipedelimiter_doublequote_enclosure TO ROLE identifier($var_role_id_analytics);
GRANT USAGE ON FILE FORMAT STAGING.file_format_csv_comma_doublequote_enclosure TO ROLE identifier($var_role_id_analytics);

-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------