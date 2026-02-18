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
USE DATABASE STRAT_DEV_DB; -- if this database is created
USE SCHEMA TECHNICAL;
-------------------------------------------------------------
CREATE OR REPLACE SEQUENCE TECHNICAL.LOG_KEY_SEQ;
CREATE OR REPLACE SEQUENCE TECHNICAL.EXTRACT_ID_SEQ;

-------------------------------------------------------------
CREATE OR REPLACE TABLE TECHNICAL.LOG (
    LOG_KEY NUMBER(38, 0) DEFAULT TECHNICAL.LOG_KEY_SEQ.NEXTVAL
    , EXTRACT_ID NUMBER(38, 0) NOT NULL
    , STEP_NAME VARCHAR(100) NOT NULL
    , SOURCE_ID VARCHAR(50) NOT NULL
    , SOURCE_FILE VARCHAR(500)
    , TARGET_TABLE VARCHAR(255)
    , STATUS VARCHAR(20) NOT NULL
    , ERROR_MESSAGE VARCHAR(4000)
    , LOADED_TS TIMESTAMP_NTZ(9) DEFAULT CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9))
    , PRIMARY KEY (LOG_KEY)
    , UNIQUE (EXTRACT_ID, STEP_NAME, SOURCE_ID)
    );
    
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
