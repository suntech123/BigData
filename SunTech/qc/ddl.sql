CREATE TABLE EDH.DOCUPACEQC_TEMP (
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQ_STAT_ACCT_STAT VARCHAR(50),
	WORKFLOW VARCHAR(50),
	REQUEST_TYPE VARCHAR(100),
	BD VARCHAR(75),
	PERSHING_REP_NUM VARCHAR(15),
	REP_NAME VARCHAR(50),
	OSJ_REP_NUM VARCHAR(20),
	OSJ_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	ACCOUNT_REGISTRATION VARCHAR(20),
	PROCESSOR_SENDING_TO_QC VARCHAR(20),
	QC_REASON_CODES VARCHAR(30),
	WORK_ITEM_ID VARCHAR(15),
	DATE_SENT_TO_QC TIMESTAMP,
	QC_PROCESSED_DATE TIMESTAMP,
	LAST_NOTE VARCHAR(200)
);

CREATE TABLE EDH.WORKFLOWQC_TEMP (
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQUEST_TYPE VARCHAR(50),
	BD VARCHAR(75),
	PERSHING_REP_NUMBER VARCHAR(15),
	REP_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	QC_REASON_CODE VARCHAR(50),
	WORK_ITEM VARCHAR(15),
	QC_COMPLETED_BY VARCHAR(15),
	QC_COMPLETE_DATE DATE,
	LAST_NOTES VARCHAR(1000)
);

CREATE TABLE EDH.OVERSIGHTLOGQC_TEMP (
	ITEM_NUMBER INTEGER,
	DATE_PROCESSED DATE,
	REVIEW_PROCESS VARCHAR(55),
	SOURCE VARCHAR(75),
	BD VARCHAR(75),
	REP_NAME VARCHAR(50),
	REP_NUM VARCHAR(50),
	ACCOUNT VARCHAR(15),
	TYPE VARCHAR(55),
	PROCESSOR VARCHAR(25),
	DETAILS_ERROR VARCHAR(1000),
	DATE_REVIEWED DATE,
	REVIEWED_BY VARCHAR(25),
	RESOLUTION VARCHAR(150)
);


CREATE TABLE EDH.PHONEQC_TEMP (
	PDATE DATE,
	NAME VARCHAR(25),
	CALL_NUM SMALLINT,
	SALESFORCE_CASE_NUM VARCHAR(10),
	TOTAL_POINTS_AVBL SMALLINT,
	TOTAL_POINTS_AWRD SMALLINT,
	QC_PERCENTAGE DECIMAL(5,2),
	GREETING_PTS VARCHAR(5),
	CALLER_VERIFICATION_PTS VARCHAR(5),
	USER_CALLERS_NAME_PTS VARCHAR(5),
	IDENTIFY_CUST_NEEDS_PTS VARCHAR(5),
	CONFIRM_UNDSTNDG_CALLERS_NEEDS_PTS VARCHAR(5),
	TELL_CALLER_U_HELP_PTS VARCHAR(5),
	SALESFORCE_CASE_CREATE_COMPLETE_PTS VARCHAR(5),
	PROCEDURES_PTS VARCHAR(5),
	UTILIZE_APPROPRIATE_RESOURCES_PTS VARCHAR(5),
	DELIVER_RESPONSE_DEFINITIVELY_CHK_UNDERSTNDG_PTS VARCHAR(5),
	OFFER_ALTERNATIVES_OPTIONS_PTS VARCHAR(5),
	APPROPRIATE_TIME_FRAMES_PTS VARCHAR(5),
	AGREEMENT_ON_RESOLUTION_PTS VARCHAR(5),
	HOLD_CONFERENCE_PTS VARCHAR(5),
	CLOSING_PTS VARCHAR(5),
	INTERPERSONAL_SKILLS_PTS VARCHAR(5),
	NOTES_PTS VARCHAR(5)
);


CREATE TABLE EDH.VELOCITYQC_TEMP (
	USERNAME VARCHAR(15),
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQUEST_TYPE VARCHAR(50),
	BD VARCHAR(75),
	PERSHING_REP_NUMBER VARCHAR(15),
	REP_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	QC_REASON_CODE VARCHAR(50),
	WORK_ITEM VARCHAR(15),
	QC_COMPLETED_BY VARCHAR(15),
	QC_COMPLETE_DATE DATE,
	LAST_NOTES VARCHAR(1000)
);


CREATE TABLE EDH.ISSUELOGQC_TEMP (
	ISSUES_LOG VARCHAR(10),
	MASTER_CALL_SUB_TYPE VARCHAR(100),
	MASTER_CALL_DETAIL VARCHAR(150),
	BD_NAME VARCHAR(75),
	CREATED_BY VARCHAR(50),
	CLIENT_ACCOUNT_NUMBER VARCHAR(100),
	CLR_FIRM_REP_NUM VARCHAR(15),
	ACCOUNT_NAME VARCHAR(70),
	CONTACT_NAME VARCHAR(50),
	DESCRIPTION VARCHAR(1000),
	REP_TEMP VARCHAR(20),
	CASE_NUMBER VARCHAR(15),
	DATE_TIME_OPENED VARCHAR(15),
	RELATED_ERROR VARCHAR(100),
	ASSOCIATE_RESPONSIBLE VARCHAR(100),
	NO_HEADER_COL VARCHAR(100),
	NOTES_IMPACT VARCHAR(1000),
	REVIEWED_BY VARCHAR(50),
	MANAGER_SUPERVISOR VARCHAR(50),
	MONTH_YEAR VARCHAR(10),
	ROOT_CAUSE_ERROR VARCHAR(200)
);


CREATE TABLE EDH.DIRECTBUSINESSQC_TEMP (
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQUEST_TYPE VARCHAR(50),
	BD VARCHAR(75),
	PERSHING_REP_NUMBER VARCHAR(15),
	REP_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	QC_REASON_CODE VARCHAR(100),
	WORK_ITEM VARCHAR(15),
	QC_COMPLETED_BY VARCHAR(15),
	QC_COMPLETE_DATE DATE,
	LAST_NOTES VARCHAR(200)
);


CREATE TABLE EDH.DOCUPACEQC (
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQ_STAT_ACCT_STAT VARCHAR(50),
	WORKFLOW VARCHAR(50),
	REQUEST_TYPE VARCHAR(100),
	BD VARCHAR(75),
	PERSHING_REP_NUM VARCHAR(15),
	REP_NAME VARCHAR(50),
	OSJ_REP_NUM VARCHAR(20),
	OSJ_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	ACCOUNT_REGISTRATION VARCHAR(20),
	PROCESSOR_SENDING_TO_QC VARCHAR(20),
	QC_REASON_CODES VARCHAR(30),
	WORK_ITEM_ID VARCHAR(15),
	DATE_SENT_TO_QC TIMESTAMP,
	QC_PROCESSED_DATE TIMESTAMP,
	LAST_NOTE VARCHAR(200)
);

CREATE TABLE EDH.WORKFLOWQC (
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQUEST_TYPE VARCHAR(50),
	BD VARCHAR(75),
	PERSHING_REP_NUMBER VARCHAR(15),
	REP_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	QC_REASON_CODE VARCHAR(50),
	WORK_ITEM VARCHAR(15),
	QC_COMPLETED_BY VARCHAR(15),
	QC_COMPLETE_DATE DATE,
	LAST_NOTES VARCHAR(1000)
);

CREATE TABLE EDH.OVERSIGHTLOGQC (
	ITEM_NUMBER INTEGER,
	DATE_PROCESSED DATE,
	REVIEW_PROCESS VARCHAR(55),
	"SOURCE" VARCHAR(75),
	BD VARCHAR(75),
	REP_NAME VARCHAR(50),
	REP_NUM VARCHAR(50),
	ACCOUNT VARCHAR(15),
	"TYPE" VARCHAR(55),
	PROCESSOR VARCHAR(25),
	DETAILS_ERROR VARCHAR(1000),
	DATE_REVIEWED DATE,
	REVIEWED_BY VARCHAR(25),
	RESOLUTION VARCHAR(150)
);


CREATE TABLE EDH.PHONEQC (
	PDATE DATE,
	NAME VARCHAR(25),
	CALL_NUM SMALLINT,
	SALESFORCE_CASE_NUM VARCHAR(10),
	TOTAL_POINTS_AVBL SMALLINT,
	TOTAL_POINTS_AWRD SMALLINT,
	QC_PERCENTAGE DECIMAL(5,2),
	GREETING_PTS VARCHAR(5),
	CALLER_VERIFICATION_PTS VARCHAR(5),
	USER_CALLERS_NAME_PTS VARCHAR(5),
	IDENTIFY_CUST_NEEDS_PTS VARCHAR(5),
	CONFIRM_UNDSTNDG_CALLERS_NEEDS_PTS VARCHAR(5),
	TELL_CALLER_U_HELP_PTS VARCHAR(5),
	SALESFORCE_CASE_CREATE_COMPLETE_PTS VARCHAR(5),
	PROCEDURES_PTS VARCHAR(5),
	UTILIZE_APPROPRIATE_RESOURCES_PTS VARCHAR(5),
	DELIVER_RESPONSE_DEFINITIVELY_CHK_UNDERSTNDG_PTS VARCHAR(5),
	OFFER_ALTERNATIVES_OPTIONS_PTS VARCHAR(5),
	APPROPRIATE_TIME_FRAMES_PTS VARCHAR(5),
	AGREEMENT_ON_RESOLUTION_PTS VARCHAR(5),
	HOLD_CONFERENCE_PTS VARCHAR(5),
	CLOSING_PTS VARCHAR(5),
	INTERPERSONAL_SKILLS_PTS VARCHAR(5),
	NOTES_PTS VARCHAR(5)
);


CREATE TABLE EDH.VELOCITYQC (
	USERNAME VARCHAR(15),
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQUEST_TYPE VARCHAR(50),
	BD VARCHAR(75),
	PERSHING_REP_NUMBER VARCHAR(15),
	REP_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	QC_REASON_CODE VARCHAR(50),
	WORK_ITEM VARCHAR(15),
	QC_COMPLETED_BY VARCHAR(15),
	QC_COMPLETE_DATE DATE,
	LAST_NOTES VARCHAR(1000)
);


CREATE TABLE EDH.ISSUELOGQC (
	ISSUES_LOG VARCHAR(10),
	MASTER_CALL_SUB_TYPE VARCHAR(100),
	MASTER_CALL_DETAIL VARCHAR(150),
	BD_NAME VARCHAR(75),
	CREATED_BY VARCHAR(50),
	CLIENT_ACCOUNT_NUMBER VARCHAR(100),
	CLR_FIRM_REP_NUM VARCHAR(15),
	ACCOUNT_NAME VARCHAR(70),
	CONTACT_NAME VARCHAR(50),
	DESCRIPTION VARCHAR(1000),
	REP_TEMP VARCHAR(20),
	CASE_NUMBER VARCHAR(15),
	DATE_TIME_OPENED VARCHAR(15),
	RELATED_ERROR VARCHAR(100),
	ASSOCIATE_RESPONSIBLE VARCHAR(100),
	NO_HEADER_COL VARCHAR(100),
	NOTES_IMPACT VARCHAR(1000),
	REVIEWED_BY VARCHAR(50),
	MANAGER_SUPERVISOR VARCHAR(50),
	MONTH_YEAR VARCHAR(10),
	ROOT_CAUSE_ERROR VARCHAR(200)
);


CREATE TABLE EDH.DIRECTBUSINESSQC (
	ASSIGNED_PROCESSOR VARCHAR(25),
	REQUEST_TYPE VARCHAR(50),
	BD VARCHAR(75),
	PERSHING_REP_NUMBER VARCHAR(15),
	REP_NAME VARCHAR(50),
	ACCOUNT_NUMBER VARCHAR(15),
	QC_REASON_CODE VARCHAR(100),
	WORK_ITEM VARCHAR(15),
	QC_COMPLETED_BY VARCHAR(15),
	QC_COMPLETE_DATE DATE,
	LAST_NOTES VARCHAR(200)
);
