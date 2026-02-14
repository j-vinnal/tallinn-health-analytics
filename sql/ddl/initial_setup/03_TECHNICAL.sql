/*
# To run this file with Snowsql:
# 0. If not done already then define the snowflake connection in file: PowerShell.exe ~\.snowsql\config
# * You can add what ever account where you have at least SYSADMIN and SECURITYADMIN roles available for the user:
# [connections.wizon_template]
# accountname = wizon_partner.eu-central-1
# username = jaanuswizon
# password = 
# 1. cd to the git project folder. The default should be then:
# * On Windows CMD>cd %userprofile%\git\repos\wizon_template\
# * On Windows PowerShell> cd ~\git\repos\wizon_template\
# 2. Make the connection:
snowsql -c wizon_template
# 3. Run the file
!load .\ddl\initial_setup\04_TECHNICAL.sql
# Or you can load those by hand:
# NB! To close snowsql connection use:
!quit
*/

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
--USE DATABASE <>_LIVE_DB;
USE SCHEMA PUBLIC;
-------------------------------------------------------------
CREATE TABLE IF NOT EXISTS TECHNICAL.LOG (
    LOG_KEY NUMBER(38, 0)
    , PARENT_LOG_KEY NUMBER(38, 0)
    , PDI_PATH VARCHAR(4000)
    , PDI_NAME VARCHAR(4000)
    , STATUS VARCHAR(100)
    , ERROR_PDI_PATH VARCHAR(4000)
    , ERROR_PDI_NAME VARCHAR(4000)
    , INSERT_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
    , UPDATE_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
    , COMMENTS VARCHAR(4000)
    );

-------------------------------------------------------------
CREATE SEQUENCE IF NOT EXISTS TECHNICAL.LOG_KEY_SEQ;
    
-------------------------------------------------------------
CREATE TABLE IF NOT EXISTS TECHNICAL.INCREMENTAL_LOAD_WINDOW (
    TABLE_SCHEMA VARCHAR(4000)
    , TABLE_NAME VARCHAR(4000)
    , LAST_LOADED_TS TIMESTAMP_NTZ(9)
    , LAST_EXEC_START_TS TIMESTAMP_NTZ(9)
    , LAST_EXEC_END_TS TIMESTAMP_NTZ(9)
    , CURRENT_LOADED_TS TIMESTAMP_NTZ(9)
    , CURRENT_EXEC_START_TS TIMESTAMP_NTZ(9)
    , CURRENT_EXEC_END_TS TIMESTAMP_NTZ(9)
    , INSERT_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
    , UPDATE_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
    , LAST_LOADED_PK INTEGER
    , CURRENT_LOADED_PK INTEGER
    );