-----------------------------------------------------------------------------------------SET SCRIPT VARIABLES
SET var_timezone = 'Europe/Tallinn';
SET var_week_start = 1; --Monday

-----------------------------------------------------------------------------------------SET ACCOUNT LEVEL ATTRIBUTES
USE ROLE ACCOUNTADMIN;

ALTER ACCOUNT SET TIMEZONE = $var_timezone;
ALTER ACCOUNT SET WEEK_START = $var_week_start;

-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------