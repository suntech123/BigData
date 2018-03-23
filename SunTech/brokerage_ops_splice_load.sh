#!/bin/bash
##########################################################################################
#
#
#
#########################################################################################

## Intializing log
timestamp=$( date '+%Y-%m-%d_%H%M%S' )
LOG_FILE="/home/splice/cetera/brokerage-operations/logs/brokerage_ops_splice_load_${timestamp}.log"
exec &>>"${LOG_FILE}"

## Assigning variables
BAD_FILE_DIR="/data/brokerage-operations/bad"
PRD_SRC_FILE_DIR="/data/brokerage-operations/BROK_OPS_PRD_PROCESSED/"
SLA_SRC_FILE_DIR="/data/brokerage-operations/BROK_OPS_SLA_PROCESSED/"
SLA_EXP_FILE_DIR="/data/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/"
ARCHIVE_FILES_DIR="/data/brokerage-operations/BROK_OPS_ARCHIVE/"

## Checking existence for source and bad file directories
echo ""
echo "[================================= Checking Directory existence in HDFS ===================================]
echo """
hadoop fs -test -d ${BAD_FILE_DIR}
[[ $? -ne 0 ]] && echo "Bad file directory doesn't exist.....exiting without loading" && exit 1

hadoop fs -test -d ${PRD_SRC_FILE_DIR}
[[ $? -ne 0 ]] && echo "HDFS PRD source file directory doesn't exist....exiting without loading" && exit 1

hadoop fs -test -d ${SLA_SRC_FILE_DIR}
[[ $? -ne 0 ]] && echo "HDFS SLA source  file directory doesn't exist.....exiting without loading" && exit 1

hadoop fs -test -d ${SLA_EXP_FILE_DIR}
[[ $? -ne 0 ]] && echo "HDFS SLA EXCEPTION file directory doesn't exist.....exiting without loading" && exit 1

hadoop fs -test -d ${ARCHIVE_FILES_DIR}
[[ $? -ne 0 ]] && echo "HDFS Brokerage ops Archive file directory doesn't exist.....exiting without loading" && exit 1


## Checking file count in source directory


## Load PRD tables from source files
echo "[====================================== PRD Temp load started ====================================]"
sqlshell.sh <<!
TRUNCATE TABLE EDH.ACTIVITY_FACT_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','ACTIVITY_FACT_TEMP',null,'/data/brokerage-operations/BROK_OPS_PRD_PROCESSED/','|',null,'yyyy-MM-dd',null,null,0,'/data/brokerage-operations/bad/',true,null);
!

echo "[====================================== PRD Fact Load Started ====================================]"
sqlshell.sh <<!
INSERT INTO EDH.ACTIVITY_FACT
SELECT TMP.ACTIVITY_DATE,
TMP.USER_ID||'|'||TMP.BD,
TMP.TEAM,
TMP.ACTIVITY,
TMP.ACTIVITY_NAME,
TMP.ACTIVITY_STATUS,
TMP.ACTIVITY_COUNT
FROM (SELECT DISTINCT TRIM(ACTIVITY_DATE) AS ACTIVITY_DATE,
TRIM(COALESCE(USER_ID,'UNKNOWN')) AS USER_ID,
TRIM(COALESCE(BD,'UNKNOWN')) AS BD,
TRIM(TEAM) TEAM,
TRIM(ACTIVITY) AS ACTIVITY,
TRIM(ACTIVITY_NAME) AS ACTIVITY_NAME,
TRIM(ACTIVITY_STATUS) AS ACTIVITY_STATUS,
TRIM(ACTIVITY_COUNT) AS ACTIVITY_COUNT
FROM EDH.ACTIVITY_FACT_TEMP ) TMP;
!

## Load SLA tables from source files
echo "[====================================== SLA Temp load started =====================================]"
sqlshell.sh <<!
TRUNCATE TABLE EDH.SLA_FACT_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','SLA_FACT_TEMP',null,'/data/brokerage-operations/BROK_OPS_SLA_PROCESSED/','|',null,'YYYY-MM-DD HH:MM:SS.SSSSSS',null,null,0,'/data/brokerage-operations/bad/',true,null);
!

echo "[====================================== SLA Fact load started ====================================]"
sqlshell.sh <<!
INSERT INTO EDH.SLA_FACT
SELECT T.SLA_DATE,
T.TEAM,
T.REQUEST_TYPE,
T.WORK_ITEM_NUMBER,
T.SLA_STATUS,
T.SLA_DURATION_HRS,
T.BD||'|'||T.ACCOUNT_STATUS||'|'||T.ACCOUNT_NUMBER,
T.SLA_DURATION,
T.SLA_VALUE,
T.SLA_START_TIME,
T.SLA_END_TIME,
T.ITEM_COMPLETED_WITHIN_SLA
FROM (SELECT DISTINCT SLA_DATE,
TRIM(TEAM) AS TEAM,
TRIM(REQUEST_TYPE) AS REQUEST_TYPE,
WORK_ITEM_NUMBER,
TRIM(SLA_STATUS) AS SLA_STATUS,
SLA_DURATION_HRS,
TRIM(SLA_DURATION) AS SLA_DURATION,
SLA_VALUE,
SLA_START_TIME,
SLA_END_TIME,
TRIM(COALESCE(BD,'UNKNOWN')) AS BD,
TRIM(COALESCE(ACCOUNT_STATUS,'UNKNOWN')) AS ACCOUNT_STATUS,
TRIM(COALESCE(ACCOUNT_NUMBER,'UNKNOWN')) AS ACCOUNT_NUMBER,
TRIM(ITEM_COMPLETED_WITHIN_SLA) AS ITEM_COMPLETED_WITHIN_SLA
FROM EDH.SLA_FACT_TEMP) T;
!

## Load SLA EXCEPTION tables from source files
echo "[==================================== SLA Exception Temp load started ======================================]"
sqlshell.sh <<!
TRUNCATE TABLE EDH.SLA_FACT_EXCEPTION_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','SLA_FACT_EXCEPTION_TEMP',null,'/data/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/','|',null,'YYYY-MM-DD HH:MM:SS.SSSSSS',null,null,0,'/data/brokerage-operations/bad/',true,null);
!

echo "[==================================== SLA Exception FAct load started ======================================]"
sqlshell.sh <<!
INSERT INTO EDH.SLA_FACT_EXCEPTION
SELECT T.SLA_DATE,
T.REQUEST_TYPE,
T.WORK_ITEM_NUMBER,
T.ACCOUNT_STATUS,
T.ACCOUNT_NUMBER,
T.BD||'|'||T.ACCOUNT_STATUS||'|'||T.ACCOUNT_NUMBER,
T.BD,
T.SLA_STATUS,
T.ITEM_COMPLETED_WITHIN_SLA,
T.SLA_START_TIME,
T.SLA_END_TIME
FROM (SELECT DISTINCT SLA_DATE,
TRIM(REQUEST_TYPE) AS REQUEST_TYPE,
WORK_ITEM_NUMBER,
TRIM(COALESCE(ACCOUNT_STATUS,'UNKNOWN')) AS ACCOUNT_STATUS,
TRIM(COALESCE(ACCOUNT_NUMBER,'UNKNOWN')) AS ACCOUNT_NUMBER,
TRIM(COALESCE(BD,'UNKNOWN')) AS BD,
TRIM(SLA_STATUS) AS SLA_STATUS,
TRIM(ITEM_COMPLETED_WITHIN_SLA) AS ITEM_COMPLETED_WITHIN_SLA,
SLA_START_TIME,
SLA_END_TIME
FROM EDH.SLA_FACT_EXCEPTION_TEMP) T;
!

echo "[====================== Starting Compaction and Gathering Statistics on all Tables ==============================]"
sqlshell.sh <<!
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('EDH','SLA_FACT_EXCEPTION');
CALL SYSCS_UTIL.COLLECT_TABLE_STATISTICS('EDH','SLA_FACT_EXCEPTION', true);
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('EDH','SLA_FACT');
CALL SYSCS_UTIL.COLLECT_TABLE_STATISTICS('EDH','SLA_FACT', true);
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('EDH','ACTIVITY_FACT');
CALL SYSCS_UTIL.COLLECT_TABLE_STATISTICS('EDH','ACTIVITY_FACT', true);
!

exit 0
