#!/bin/bash
#####################################################################################################
#
#
#
#
#
#####################################################################################################
## Intializing log
timestamp=$( date '+%Y-%m-%d_%H%M%S' )
LOG_FILE="/home/splice/cetera/brokerage-operations-quality-center/logs/brokerage_ops_quality_ceneter_splice_load_${timestamp}.log"
exec &>>"${LOG_FILE}"

## Load table DIRECTBUSINESSQC
echo ""
echo "Started Loading DIRECTBUSINESSQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.DIRECTBUSINESSQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','DIRECTBUSINESSQC_TEMP',null,'/data/brokerage-operations-quality-center/DIRECTBUSINESSQC_STG','|',null,null,'MM/dd/yyyy',null,0,'/bad/qc/abc',true,null);
!
## Loading actual table 
sqlshell.sh<<!
INSERT INTO EDH.DIRECTBUSINESSQC
SELECT TMP.ASSIGNED_PROCESSOR,TMP.REQUEST_TYPE,
TMP.BD,TMP.PERSHING_REP_NUMBER,TMP.REP_NAME,
TMP.ACCOUNT_NUMBER,TMP.QC_REASON_CODE,TMP.WORK_ITEM,
TMP.QC_COMPLETED_BY,TMP.QC_COMPLETE_DATE,TMP.LAST_NOTES
FROM (SELECT DISTINCT ASSIGNED_PROCESSOR,
REQUEST_TYPE,BD,PERSHING_REP_NUMBER,REP_NAME,ACCOUNT_NUMBER,QC_REASON_CODE,
WORK_ITEM,QC_COMPLETED_BY,QC_COMPLETE_DATE,LAST_NOTES
FROM EDH.DIRECTBUSINESSQC_TEMP ) TMP;
!

## Load data into tables
echo ""
echo "Started Loading DOCUPACEQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.DOCUPACEQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','DOCUPACEQC_TEMP',null,'/data/brokerage-operations-quality-center/DOCUPACEQC_STG','|',null,'MM/dd/yyyy hh:mm',null,null,-1,'/bad/qc/abc',true,null);
!
## Load data in table 
sqlshell.sh<<!
INSERT INTO EDH.DOCUPACEQC
SELECT TMP.ASSIGNED_PROCESSOR,
TMP.REQ_STAT_ACCT_STAT,TMP.WORKFLOW,TMP.REQUEST_TYPE,TMP.BD,          
TMP.PERSHING_REP_NUM,TMP.REP_NAME,TMP.OSJ_REP_NUM,            
TMP.OSJ_NAME,TMP.ACCOUNT_NUMBER,TMP.ACCOUNT_REGISTRATION,   
TMP.PROCESSOR_SENDING_TO_QC,TMP.QC_REASON_CODES,TMP.WORK_ITEM_ID,           
TMP.DATE_SENT_TO_QC,TMP.QC_PROCESSED_DATE,TMP.LAST_NOTE
FROM (SELECT DISTINCT ASSIGNED_PROCESSOR,
REQ_STAT_ACCT_STAT,WORKFLOW,REQUEST_TYPE,BD,          
PERSHING_REP_NUM,REP_NAME,OSJ_REP_NUM,            
OSJ_NAME,ACCOUNT_NUMBER,ACCOUNT_REGISTRATION,   
PROCESSOR_SENDING_TO_QC,QC_REASON_CODES,WORK_ITEM_ID,           
DATE_SENT_TO_QC,QC_PROCESSED_DATE,LAST_NOTE
FROM EDH.DOCUPACEQC_TEMP ) TMP;
!

## Load data into temp table
echo ""
echo "Started Loading WORKFLOWQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.WORKFLOWQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA('EDH','WORKFLOWQC_TEMP',null,'/data/brokerage-operations-quality-center/WORKFLOWQC_STG','|',null,null,'MM/dd/yyyy',null,0,'/bad/qc/abc',true,null);
!

## Load target table
sqlshell.sh<<!
INSERT INTO EDH.WORKFLOWQC
SELECT TMP.ASSIGNED_PROCESSOR 
,TMP.REQUEST_TYPE,TMP.BD,TMP.PERSHING_REP_NUMBER
,TMP.REP_NAME,TMP.ACCOUNT_NUMBER,TMP.QC_REASON_CODE     
,TMP.WORK_ITEM,TMP.QC_COMPLETED_BY,TMP.QC_COMPLETE_DATE   
,TMP.LAST_NOTES
FROM (SELECT DISTINCT ASSIGNED_PROCESSOR 
,REQUEST_TYPE,BD,PERSHING_REP_NUMBER
,REP_NAME,ACCOUNT_NUMBER,QC_REASON_CODE     
,WORK_ITEM,QC_COMPLETED_BY,QC_COMPLETE_DATE   
,LAST_NOTES
FROM EDH.WORKFLOWQC_TEMP ) TMP;
!

## Load data into temp table
echo ""
echo "Started Loading OVERSIGHTLOGQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.OVERSIGHTLOGQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA('EDH','OVERSIGHTLOGQC_TEMP',null,'/data/brokerage-operations-quality-center/OVERSIGHTLOGQC_STG','|',null,null,'MM/dd/yyyy',null,0,'/bad/qc/abc',true,null);
!

## Load target table
sqlshell.sh<<!
INSERT INTO EDH.OVERSIGHTLOGQC
SELECT TMP.ITEM_NUMBER,TMP.DATE_PROCESSED,
TMP.REVIEW_PROCESS,TMP.SOURCE,
TMP.BD,TMP.REP_NAME,TMP.REP_NUM,
TMP.ACCOUNT,TMP.TYPE,TMP.PROCESSOR,
TMP.DETAILS_ERROR,TMP.DATE_REVIEWED,
TMP.REVIEWED_BY,TMP.RESOLUTION
FROM (SELECT DISTINCT ITEM_NUMBER,
DATE_PROCESSED,REVIEW_PROCESS,SOURCE,BD,REP_NAME,REP_NUM,
ACCOUNT,TYPE,PROCESSOR,DETAILS_ERROR,DATE_REVIEWED,REVIEWED_BY,
RESOLUTION
FROM EDH.OVERSIGHTLOGQC_TEMP ) TMP;
!
## Load data into temp table
echo ""
echo "Started Loading PHONEQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.PHONEQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA('EDH','PHONEQC_TEMP',null,'/data/brokerage-operations-quality-center/PHONEQC_STG','|',null,null,'MM/dd/yyyy',null,0,'/bad/qc/abc',true,null);
!

## Load target table
sqlshell.sh<<!
INSERT INTO EDH.PHONEQC
SELECT TMP.ASSOCIATE_NAME,TMP.CALL_NUMBER,TMP.DATE_OF_CALL,TMP.DATE_OF_QC,TMP.PERC_SCORE
FROM (SELECT DISTINCT ASSOCIATE_NAME,CALL_NUMBER,DATE_OF_CALL,DATE_OF_QC,PERC_SCORE
FROM EDH.PHONEQC_TEMP ) TMP;
!

## Load temp table
echo ""
echo "Started Loading VELOCITYQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.VELOCITYQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','VELOCITYQC_TEMP',null,'/data/brokerage-operations-quality-center/VELOCITYQC_STG','|',null,null,'MM/dd/yyyy',null,0,'/bad/qc/abc',true,null);
!

## Load target table
sqlshell.sh<<!
INSERT INTO EDH.VELOCITYQC
SELECT TMP.ASSIGNED_PROCESSOR,TMP.
REQUEST_TYPE,TMP.BD,TMP.PERSHING_REP_NUMBER,TMP.REP_NAME,TMP.
ACCOUNT_NUMBER,TMP.QC_REASON_CODE,TMP.WORK_ITEM,TMP.
QC_COMPLETED_BY,TMP.QC_COMPLETE_DATE,TMP.LAST_NOTES
FROM (SELECT DISTINCT ASSIGNED_PROCESSOR,
REQUEST_TYPE,BD,PERSHING_REP_NUMBER,REP_NAME,
ACCOUNT_NUMBER,QC_REASON_CODE,WORK_ITEM,
QC_COMPLETED_BY,QC_COMPLETE_DATE,LAST_NOTES
FROM EDH.VELOCITYQC_TEMP ) TMP;
!

## Load temp table
echo ""
echo "Started Loading ISSUELOGQC..................."
echo ""
sqlshell.sh<<!
TRUNCATE TABLE EDH.ISSUELOGQC_TEMP;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','ISSUELOGQC_TEMP',null,'/data/brokerage-operations-quality-center/ISSUELOGQC_STG','|',null,null,'MM/dd/yy',null,-1,'/bad/qc/abc',true,null);
!

## LOad Target table
sqlshell.sh<<!
INSERT INTO EDH.ISSUELOGQC
SELECT TMP.ISSUES_LOG,TMP.MASTER_CALL_SUB_TYPE,TMP.MASTER_CALL_DETAIL,TMP.BD_NAME,TMP.
CREATED_BY,TMP.CLIENT_ACCOUNT_NUMBER,TMP.CLR_FIRM_REP_NUM,TMP.ACCOUNT_NAME,TMP.
CONTACT_NAME,TMP.DESCRIPTION,TMP.REP_TEMP,TMP.CASE_NUMBER,TMP.DATE_TIME_OPENED,TMP.
RELATED_ERROR,TMP.ASSOCIATE_RESPONSIBLE,TMP.NOTES_IMPACT,TMP.
REVIEWED_BY,TMP.MANAGER_SUPERVISOR,TMP.MONTH_YEAR,TMP.ROOT_CAUSE_ERROR
FROM (SELECT DISTINCT ISSUES_LOG,MASTER_CALL_SUB_TYPE,MASTER_CALL_DETAIL,BD_NAME,
CREATED_BY,CLIENT_ACCOUNT_NUMBER,CLR_FIRM_REP_NUM,ACCOUNT_NAME,
CONTACT_NAME,DESCRIPTION,REP_TEMP,CASE_NUMBER,DATE_TIME_OPENED,
RELATED_ERROR,ASSOCIATE_RESPONSIBLE,NOTES_IMPACT,
REVIEWED_BY,MANAGER_SUPERVISOR,MONTH_YEAR,ROOT_CAUSE_ERROR
FROM EDH.ISSUELOGQC_TEMP ) TMP;
!

exit 0
