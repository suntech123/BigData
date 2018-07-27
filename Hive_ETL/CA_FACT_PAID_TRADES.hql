/*
##################################################################################################################
Purpose: 
		1. To Load data into FACT_paid_trades table for BD CA
##################################################################################################################
*/
ELAPSEDTIME ON;

	/* Truncate and Reload the TEMP_CA_MAX_PAID_DATE stage tables - Step 1 */

TRUNCATE TABLE EDH_STAGING.TEMP_CA_MAX_PAID_DATE;
		
	/* Truncate and Reload the TEMP_CA_TRADE stage tables - Step 2 */
	
TRUNCATE TABLE EDH_STAGING.TEMP_CA_TRADE;
	
	/* Truncate and Reload the TEMP_CA_TRADE_T2 stage tables - Step 3 */
	
TRUNCATE TABLE EDH_STAGING.TEMP_CA_TRADE_T2;
	
	/* Truncate and Reload the FACT_PAID_TRADES_CA stage tables - Step 4 */
	
TRUNCATE TABLE EDH_STAGING.FACT_PAID_TRADES_CA;
	
	/* Truncate FACT_PAID_TRADES_PAYOUT_CA stage table - Step 5 */
	
TRUNCATE TABLE EDH_STAGING.FACT_PAID_TRADES_PAYOUT_CA;	

	/* Insert Max(PD_DATE) from Trades to select only incremented records in the main query */
	
INSERT INTO EDH_STAGING.TEMP_CA_MAX_PAID_DATE (BD_PARTY_ID, MAX_PAID_DATE) 
SELECT BD AS BD_PARTY_ID,
			MAX( PD_DATE ) MAX_PAID_DATE
		FROM
			BONUS.TRADES T --splice-properties useSpark=true
		WHERE
			BD = 4
		GROUP BY BD;

	/* THIS UPDATE STATEMENT HAVE TO BE UN-COMMENTED AFTER FIRST TIME RUN. */
	
UPDATE EDH_STAGING.TEMP_CA_MAX_PAID_DATE PD
   SET PD.LAST_USED_TRADES_KEY =
       (SELECT MAX(T.FACT_PAID_TRADES_KEY)
         FROM EDH.FACT_PAID_TRADES T --splice-properties useSpark=true
         WHERE T.BD_ID = 4),
       PD.LAST_USED_PAYOUT_KEY =
       (SELECT MAX(P.FACT_PAID_TRADES_PAYOUT_KEY)
         FROM EDH.FACT_PAID_TRADES_PAYOUT P --splice-properties useSpark=true
         WHERE P.BD_ID = 4)
WHERE PD.BD_PARTY_ID = 4;

CALL SYSCS_UTIL.COLLECT_TABLE_STATISTICS('EDH_STAGING','TEMP_CA_MAX_PAID_DATE', true);
ANALYZE table EDH_STAGING.TEMP_CA_MAX_PAID_DATE;

	/* Insert only the incremental data into a TEMP table */
	
INSERT INTO EDH_STAGING.TEMP_CA_TRADE 
SELECT T.TRADE_ID,
			TRIM( CAST( T.TRADE_ID AS CHAR( 38 ))) AS SRC_TRADE_ID,
			T.ACCT_ID,
			T.TRADE_DATE AS TRADE_DT,
			T.PD_DATE PAID_DT,
			T.BD AS BD_ID,
			C.SSN,
			T.SPONSOR_ACCT_NUMBER AS SPONS_ACCOUNT_NO,
			TRIM( REPLACE( REPLACE( LTRIM( REPLACE( T.SPONSOR_ACCT_NUMBER, '0', ' ' )), ' ', '0' ), '-', '' )) AS TRIMMED_SPONS_ACCOUNT_NO,
			COALESCE(T.ACTUAL_CUSIP,P.CUSIP) AS CUSIP,
			T.RNR AS REP_NUMBER_OF_RECORD, 
			TRIM(CASE
				WHEN T.A12B1_IND = 'Y' AND P.GROUP_CODE <> 'ADV' THEN 'TRAIL' 
				WHEN P.GROUP_CODE = 'ADV' 
				THEN 'ADVISORY_FEE'
				WHEN T.GDC_CREDIT_ONLY_IND = 'Y' THEN 'GDC_CREDIT_ONLY'
				WHEN T.FEE_CODE = 'EXCH' THEN 'EXCHANGE'
				WHEN T.BUY_SELL_IND = 'B' THEN 'BUY'
				WHEN T.BUY_SELL_IND = 'S' THEN 'SELL'
				ELSE 'OTHERS'
			END) AS TRANS_TYPE,
			CASE WHEN TRIM(C.FIRST_NAME) IS NULL AND TRIM(C.LAST_NAME) IS NULL
			THEN TRIM(ALPHA_INDEX)
			ELSE TRIM(COALESCE(C.FIRST_NAME,'') || COALESCE( ' ' || C.MIDDLE_INITIAL,'') ||  COALESCE(' ' ||C.LAST_NAME,'')) END AS CLIENT_NM,
			T.PRODUCT_NAME AS PRODUCT_NM,
			T.CANCEL_DATE AS CANCEL_DT,
			CAST(T.RECEIVE_DATE AS DATE) AS RECEIVE_DT,
			CASE WHEN T.SETTLEMENT_DATE IS NULL THEN T.TRADE_DATE
				 ELSE t.SETTLEMENT_DATE
			END AS SETTLEMENT_DT,
			T.UNITS AS QUANTITY,
			T.UNITS_PRICE AS PRICE,
			T.GDC_AMT AS GDC_AMT,
			T.GDC_AMT AS REPORTED_GROSS_GDC_AMT,
			T.BUY_SELL_IND,
			T.T.A12B1_IND,
			P.GROUP_CODE,
			T.GDC_CREDIT_ONLY_IND,
			T.FEE_CODE,
			T.SPON_CODE,
			CASE WHEN (T.BUY_SELL_IND ='B' 
                                AND NOT  (T.A12B1_IND = 'Y' 
				AND (P.GROUP_CODE = 'ADV' AND T.A12B1_IND = 'N')
                                AND T.GDC_CREDIT_ONLY_IND='Y'  
				AND T.BUY_SELL_IND ='S'   
				AND T.FEE_CODE = 'EXCH' ) 
			  )  THEN  T.INVESTMENT_AMT 
			  ELSE  0
			END AS SALES_AMT,
			T.INVESTMENT_AMT AS INVESTMENT_AMT,
			TRIM(CASE WHEN T.FEE_CODE = 'NTF' THEN 'Y'
				 ELSE 'N'
			END) AS TICKET_FEE_WAIVED_FLG,
			T.INCOMING_FEE_AMT AS INCOMMING_TICKET_FEE_AMT,
			T.FEE_AMT AS TICKET_FEE_AMT,
			T.ELECTRONIC_TRADE_IND AS ELECTRONIC_TRADE_IND,
			TRIM(CASE WHEN T.SPON_CODE = '910' THEN 'Y'
				 ELSE 'N'
			END) AS BROKERAGE_FLG,
			TRIM( CAST( T.CONFIRM_NUMBER AS CHAR( 38 ))) AS TRADE_CONFIRM_NUMBER,
			TRIM(CASE WHEN T.SPON_CODE <> '910'
				 AND P.GROUP_CODE <> 'ADV' THEN 'DIRECT'
				 WHEN T.SPON_CODE <> '910'
				 AND P.GROUP_CODE = 'ADV' THEN 'TURNKEY ASSET MANAGEMENT PROGRAM'
				 WHEN T.SPON_CODE = '910'
				 AND P.GROUP_CODE = 'ADV' THEN 'ADVISORY'
				 WHEN T.SPON_CODE = '910'
				 AND P.GROUP_CODE <> 'ADV' THEN 'BROKERAGE'
				 ELSE 'UNKNOWN'
			END) AS SRC_LOB
		FROM
			BONUS.TRADES T --splice-properties useSpark=true
		INNER JOIN EDH_STAGING.TEMP_CA_MAX_PAID_DATE MP ON
			MP.BD_PARTY_ID = T.BD
		INNER JOIN EDH.DIM_BD BDM ON
			BDM.BD_ID = T.BD
			AND T.PD_DATE > BDM.LST_PAID_DT
			AND MP.MAX_PAID_DATE > BDM.LST_PAID_DT 
			AND T.BD = 4 
		LEFT JOIN BONUS.PRODUCTS P ON
			UPPER(TRIM(T.PROD_SPON_CODE)) = UPPER(TRIM(P.SPON_CODE))
			AND UPPER(TRIM(T.PRODUCT_NUMBER)) = UPPER(TRIM(P.PRODUCT_NUMBER))
		LEFT JOIN BONUS.CLIENTS C ON
			T.CLIENT_ID = C.CLIENT_ID
			AND T.BD = C.BD
		WHERE T.BD = 4; 
			
	/* Collect stats and compaction on the tables*/
			
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('EDH_STAGING','TEMP_CA_TRADE');

ANALYZE TABLE EDH_STAGING.TEMP_CA_TRADE;

	/* Both the below 3 inserts into TEMP2 table is to get DIM_KEY values*/
	/*Insert records for CASE 1 as per Accounts length = 9 on both sides (Use Inner joins for both DIM_ACCOUNTs and DIM_CLIENT to pull only matching records) */

INSERT INTO EDH_STAGING.TEMP_CA_TRADE_T2 
SELECT
T.TRADE_ID,
T.SRC_TRADE_ID,
T.ACCT_ID,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
T.SSN,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
T.CLIENT_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
T.QUANTITY,
T.PRICE,
T.GDC_AMT,
T.REPORTED_GROSS_GDC_AMT,
T.BUY_SELL_IND,
T.A12B1_IND,
T.GROUP_CODE,
T.GDC_CREDIT_ONLY_IND,
T.FEE_CODE,
T.SPON_CODE,
T.SALES_AMT,
T.INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
T.INCOMMING_TICKET_FEE_AMT,
T.TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
T.SRC_LOB,
T.DIM_ACCOUNTS_KEY,
T.BD_SOURCE,
T.DATA_SOURCE,
T.LOB,
T.SPONSOR_NM,
T.STATUS,
T.TAX_ID_NB,
T.AD_TRIMMED_SPONS_ACCOUNT_NO,
T.AD_SPONS_ACCOUNT_NO,
T.DIM_CLIENT_KEY,
T.CD_CLIENT_NM,
T.DIM_REP_NUMBER_KEY,
T.DIM_BRANCH_KEY,
ROW_NUMBER() OVER(PARTITION BY T.BD_ID,	T.TRADE_ID ORDER BY T.STATUS ASC, T.ACCT_ID DESC, T.DIM_ACCOUNTS_KEY DESC ) RNK,
1 AS CASES FROM
(SELECT
T.TRADE_ID,
T.SRC_TRADE_ID,
T.ACCT_ID,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
T.SSN,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
T.CLIENT_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
T.QUANTITY,
T.PRICE,
T.GDC_AMT,
T.REPORTED_GROSS_GDC_AMT,
T.BUY_SELL_IND,
T.A12B1_IND,
T.GROUP_CODE,
T.GDC_CREDIT_ONLY_IND,
T.FEE_CODE,
T.SPON_CODE,
T.SALES_AMT,
T.INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
T.INCOMMING_TICKET_FEE_AMT,
T.TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
T.SRC_LOB,
AD.DIM_ACCOUNTS_KEY,
AD.BD_SOURCE,
AD.DATA_SOURCE,
AD.LOB,
AD.SPONSOR_NM,
AD.STATUS,
AD.TAX_ID_NB,
AD.TRIMMED_SPONS_ACCOUNT_NO AS AD_TRIMMED_SPONS_ACCOUNT_NO,
AD.SPONS_ACCOUNT_NO AS AD_SPONS_ACCOUNT_NO,
CD.DIM_CLIENT_KEY,
NULLIF('','') as CD_CLIENT_NM,
DR.DIM_REP_NUMBER_KEY,
DB.DIM_BRANCH_KEY
		FROM
			EDH_STAGING.TEMP_CA_TRADE T --splice-properties useSpark=true
			INNER JOIN EDH.DIM_BD BDM 
 ON T.BD_ID=BDM.BD_ID
 AND BDM.BD_ID=4
			INNER JOIN (SELECT DIM_CLIENT_KEY,DIM_ACCOUNTS_KEY,BD_ID, TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER, SRC_CLIENT_ID, RNK1,
BD_SOURCE,DATA_SOURCE,LOB,SPONSOR_NM,STATUS,SPONS_ACCOUNT_NO 
		FROM (SELECT TRIM(CAST(BD_ID AS CHAR(2)))||SRC_CLIENT_ID AS DIM_CLIENT_KEY,DIM_ACCOUNTS_KEY,BD_ID, TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER, SRC_CLIENT_ID, BD_SOURCE,
DATA_SOURCE,LOB,SPONSOR_NM,STATUS,SPONS_ACCOUNT_NO, ROW_NUMBER() OVER(PARTITION BY TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER ORDER BY STATUS, SRC_ACCOUNT_ID DESC) AS RNK1 
FROM EDH_STAGING.DIM_ACCOUNT_CA) AD
WHERE RNK1 =1)  AD
 ON
			T.BD_ID = AD.BD_ID
			AND T.TRIMMED_SPONS_ACCOUNT_NO = AD.TRIMMED_SPONS_ACCOUNT_NO
			AND UPPER(TRIM(T.SSN)) = UPPER(TRIM(AD.TAX_ID_NB))
			AND UPPER(TRIM(T.REP_NUMBER_OF_RECORD)) = UPPER(TRIM(AD.REP_NUMBER ))
		INNER JOIN EDH_STAGING.DIM_CLIENT_CA CD --splice-properties joinStrategy=SORTMERGE
 ON
			T.BD_ID = CD.BD_ID
			AND UPPER(TRIM(T.SSN)) = UPPER(TRIM(CD.TAX_ID_NB))  
		INNER JOIN (SELECT PAID_DATE, TRADE_ID, OFFICE_CODE, ROW_NUMBER() OVER(PARTITION BY TRADE_ID ORDER BY OFFICE_CODE) AS RNK2 FROM BONUS.TRADECOMM T
		WHERE T.PAID_DATE IS NOT NULL
		) TC
			ON T.TRADE_ID=TC.TRADE_ID
			AND TC.RNK2=1
			AND TC.PAID_DATE > BDM.LST_PAID_DT 
		LEFT JOIN EDH_STAGING.DIM_BRANCH_CA DB 
			ON UPPER(TRIM(TC.OFFICE_CODE))= UPPER(TRIM(DB.BRANCH_ID ))
		LEFT JOIN EDH_STAGING.DIM_REP_NUMBER_CA DR --splice-properties joinStrategy=BROADCAST
 ON
			DR.BD_ID = T.BD_ID
			AND UPPER(TRIM(T.REP_NUMBER_OF_RECORD)) = UPPER(TRIM(DR.REP_NUMBER ))
		WHERE
			LENGTH(TRIM(T.SSN))= 9
			AND LENGTH(TRIM(AD.TAX_ID_NB))= 9) T;

	/* Insert records for CASE 2 as per Accounts length <> 9 and on both sides not falling in CASE 1(Using Left join for DIM_CLIENT to pull records that are not picked in CASE 1) */
			
INSERT INTO EDH_STAGING.TEMP_CA_TRADE_T2 
SELECT
T.TRADE_ID,
T.SRC_TRADE_ID,
T.ACCT_ID,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
T.SSN,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
T.CLIENT_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
T.QUANTITY,
T.PRICE,
T.GDC_AMT,
T.REPORTED_GROSS_GDC_AMT,
T.BUY_SELL_IND,
T.A12B1_IND,
T.GROUP_CODE,
T.GDC_CREDIT_ONLY_IND,
T.FEE_CODE,
T.SPON_CODE,
T.SALES_AMT,
T.INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
T.INCOMMING_TICKET_FEE_AMT,
T.TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
T.SRC_LOB,
T.DIM_ACCOUNTS_KEY,
T.BD_SOURCE,
T.DATA_SOURCE,
T.LOB,
T.SPONSOR_NM,
T.STATUS,
T.TAX_ID_NB,
T.AD_TRIMMED_SPONS_ACCOUNT_NO,
T.AD_SPONS_ACCOUNT_NO,
T.DIM_CLIENT_KEY,
T.CD_CLIENT_NM,
T.DIM_REP_NUMBER_KEY,
T.DIM_BRANCH_KEY,
ROW_NUMBER() OVER(PARTITION BY T.BD_ID,	T.TRADE_ID ORDER BY T.STATUS ASC, T.ACCT_ID DESC, T.DIM_ACCOUNTS_KEY DESC ) RNK,
2 AS CASES FROM
(SELECT
T.TRADE_ID,
T.SRC_TRADE_ID,
T.ACCT_ID,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
T.SSN,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
T.CLIENT_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
T.QUANTITY,
T.PRICE,
T.GDC_AMT,
T.REPORTED_GROSS_GDC_AMT,
T.BUY_SELL_IND,
T.A12B1_IND,
T.GROUP_CODE,
T.GDC_CREDIT_ONLY_IND,
T.FEE_CODE,
T.SPON_CODE,
T.SALES_AMT,
T.INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
T.INCOMMING_TICKET_FEE_AMT,
T.TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
T.SRC_LOB,
AD.DIM_ACCOUNTS_KEY,
AD.BD_SOURCE,
AD.DATA_SOURCE,
AD.LOB,
AD.SPONSOR_NM,
AD.STATUS,
AD.TAX_ID_NB,
AD.TRIMMED_SPONS_ACCOUNT_NO AS AD_TRIMMED_SPONS_ACCOUNT_NO,
AD.SPONS_ACCOUNT_NO AS AD_SPONS_ACCOUNT_NO,
CD.DIM_CLIENT_KEY,
NULLIF('','') as CD_CLIENT_NM,
DR.DIM_REP_NUMBER_KEY,
DB.DIM_BRANCH_KEY
		FROM
			EDH_STAGING.TEMP_CA_TRADE T --splice-properties useSpark=true
						INNER JOIN EDH.DIM_BD BDM 
 ON T.BD_ID=BDM.BD_ID
 AND BDM.BD_ID=4
		INNER JOIN (SELECT DIM_CLIENT_KEY,DIM_ACCOUNTS_KEY,BD_ID, TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER, SRC_CLIENT_ID, RNK1,
BD_SOURCE,DATA_SOURCE,LOB,SPONSOR_NM,STATUS,SPONS_ACCOUNT_NO 
		FROM (SELECT TRIM(CAST(BD_ID AS CHAR(2)))||SRC_CLIENT_ID AS DIM_CLIENT_KEY,DIM_ACCOUNTS_KEY,BD_ID, TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER, SRC_CLIENT_ID, BD_SOURCE,
DATA_SOURCE,LOB,SPONSOR_NM,STATUS,SPONS_ACCOUNT_NO, ROW_NUMBER() OVER(PARTITION BY TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER ORDER BY STATUS, SRC_ACCOUNT_ID DESC) AS RNK1 
FROM EDH_STAGING.DIM_ACCOUNT_CA) AD1
WHERE RNK1 =1)  AD	
		ON T.BD_ID = AD.BD_ID
			AND T.TRIMMED_SPONS_ACCOUNT_NO = AD.TRIMMED_SPONS_ACCOUNT_NO
			AND UPPER(TRIM(T.REP_NUMBER_OF_RECORD)) = UPPER(TRIM(AD.REP_NUMBER))
		INNER JOIN (SELECT PAID_DATE, TRADE_ID, OFFICE_CODE, ROW_NUMBER() OVER(PARTITION BY TRADE_ID ORDER BY OFFICE_CODE) AS RNK2 FROM BONUS.TRADECOMM T
		WHERE T.PAID_DATE IS NOT NULL 
			) TC
			ON T.TRADE_ID=TC.TRADE_ID
			AND TC.PAID_DATE > BDM.LST_PAID_DT
			AND TC.RNK2=1
		LEFT JOIN EDH_STAGING.DIM_BRANCH_CA DB 
			ON UPPER(TRIM(TC.OFFICE_CODE))= UPPER(TRIM(DB.BRANCH_ID ))
		LEFT JOIN EDH_STAGING.DIM_CLIENT_CA CD --splice-properties joinStrategy=SORTMERGE
			ON T.BD_ID = CD.BD_ID
			AND UPPER(TRIM(AD.TAX_ID_NB)) = UPPER(TRIM(CD.TAX_ID_NB))
		LEFT JOIN EDH_STAGING.DIM_REP_NUMBER_CA DR --splice-properties joinStrategy=BROADCAST
			ON DR.BD_ID = T.BD_ID
			AND UPPER(TRIM(T.REP_NUMBER_OF_RECORD)) = UPPER(TRIM(DR.REP_NUMBER)) 
		WHERE NOT EXISTS(
				SELECT 1 FROM EDH_STAGING.TEMP_CA_TRADE_T2 T2
		WHERE T.TRADE_ID = T2.TRADE_ID
		      AND T.BD_ID = T2.BD_ID
			  ) AND (CASE WHEN LENGTH(TRIM(SSN)) <> 9 THEN 'Y'
			              WHEN SSN IS NULL THEN 'Y'
						  WHEN Length(TRIM(SSN))=9 AND Length(TRIM(AD.TAX_ID_NB))=9 AND UPPER(TRIM(SSN)) <> UPPER(TRIM(AD.TAX_ID_NB)) THEN 'N'
						  ELSE 'N' END) = 'Y') T; 

	/*Insert records for CASE 3 to push records into TEMP that are not falling in CASE 1 & 2(Using Left join for both DIM_ACCOUNTs and DIM_CLIENT to pull records that are not picked in CASE 1 & 2 where Inner Joins are used) */
			
INSERT INTO EDH_STAGING.TEMP_CA_TRADE_T2 
SELECT
T.TRADE_ID,
T.SRC_TRADE_ID,
T.ACCT_ID,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
T.SSN,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
T.CLIENT_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
T.QUANTITY,
T.PRICE,
T.GDC_AMT,
T.REPORTED_GROSS_GDC_AMT,
T.BUY_SELL_IND,
T.A12B1_IND,
T.GROUP_CODE,
T.GDC_CREDIT_ONLY_IND,
T.FEE_CODE,
T.SPON_CODE,
T.SALES_AMT,
T.INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
T.INCOMMING_TICKET_FEE_AMT,
T.TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
T.SRC_LOB,
T.DIM_ACCOUNTS_KEY,
T.BD_SOURCE,
T.DATA_SOURCE,
T.LOB,
T.SPONSOR_NM,
T.STATUS,
T.TAX_ID_NB,
T.AD_TRIMMED_SPONS_ACCOUNT_NO,
T.AD_SPONS_ACCOUNT_NO,
T.DIM_CLIENT_KEY,
T.CD_CLIENT_NM,
T.DIM_REP_NUMBER_KEY,
T.DIM_BRANCH_KEY,
ROW_NUMBER() OVER(PARTITION BY T.BD_ID,	T.TRADE_ID ORDER BY T.STATUS ASC, T.ACCT_ID DESC, T.DIM_ACCOUNTS_KEY DESC ) RNK,
3 AS CASES FROM(
SELECT
T.TRADE_ID,
T.SRC_TRADE_ID,
T.ACCT_ID,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
T.SSN,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
T.CLIENT_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
T.QUANTITY,
T.PRICE,
T.GDC_AMT,
T.REPORTED_GROSS_GDC_AMT,
T.BUY_SELL_IND,
T.A12B1_IND,
T.GROUP_CODE,
T.GDC_CREDIT_ONLY_IND,
T.FEE_CODE,
T.SPON_CODE,
T.SALES_AMT,
T.INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
T.INCOMMING_TICKET_FEE_AMT,
T.TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
T.SRC_LOB,
CASE WHEN Length(TRIM(SSN))=9 AND Length(TRIM(AD.TAX_ID_NB))=9 AND UPPER(TRIM(SSN)) <> UPPER(TRIM(AD.TAX_ID_NB)) 
THEN NULLIF('','') 
ELSE AD.DIM_ACCOUNTS_KEY END as DIM_ACCOUNTS_KEY,
AD.BD_SOURCE,
AD.DATA_SOURCE,
CASE WHEN Length(TRIM(SSN))=9 AND Length(TRIM(AD.TAX_ID_NB))=9 AND UPPER(TRIM(SSN)) <> UPPER(TRIM(AD.TAX_ID_NB)) 
THEN NULLIF('','') 
ELSE AD.LOB END as LOB,
AD.SPONSOR_NM,
AD.STATUS,
AD.TAX_ID_NB,
AD.TRIMMED_SPONS_ACCOUNT_NO AS AD_TRIMMED_SPONS_ACCOUNT_NO,
AD.SPONS_ACCOUNT_NO AS AD_SPONS_ACCOUNT_NO,
CD.DIM_CLIENT_KEY,
NULLIF('','') AS CD_CLIENT_NM,
DR.DIM_REP_NUMBER_KEY,
DB.DIM_BRANCH_KEY
		FROM
			EDH_STAGING.TEMP_CA_TRADE T --splice-properties useSpark=true
						INNER JOIN EDH.DIM_BD BDM 
 ON T.BD_ID=BDM.BD_ID
 AND BDM.BD_ID=4
		INNER JOIN (SELECT PAID_DATE,TRADE_ID, OFFICE_CODE, ROW_NUMBER() OVER(PARTITION BY TRADE_ID ORDER BY OFFICE_CODE) AS RNK2 FROM BONUS.TRADECOMM T
		WHERE T.PAID_DATE IS NOT NULL 
		) TC
			ON T.TRADE_ID=TC.TRADE_ID
			AND TC.PAID_DATE > BDM.LST_PAID_DT
			AND TC.RNK2=1
		LEFT JOIN EDH_STAGING.DIM_BRANCH_CA DB --splice-properties joinStrategy=SORTMERGE
			ON UPPER(TRIM(TC.OFFICE_CODE))= UPPER(TRIM(DB.BRANCH_ID ))
		LEFT JOIN (SELECT DIM_CLIENT_KEY,DIM_ACCOUNTS_KEY,BD_ID, TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER, SRC_CLIENT_ID, RNK1,
BD_SOURCE,DATA_SOURCE,LOB,SPONSOR_NM,STATUS,SPONS_ACCOUNT_NO 
		FROM (SELECT TRIM(CAST(BD_ID AS CHAR(2)))||SRC_CLIENT_ID AS DIM_CLIENT_KEY,DIM_ACCOUNTS_KEY,BD_ID, TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER, SRC_CLIENT_ID, BD_SOURCE,
DATA_SOURCE,LOB,SPONSOR_NM,STATUS,SPONS_ACCOUNT_NO, ROW_NUMBER() OVER(PARTITION BY TRIMMED_SPONS_ACCOUNT_NO, TAX_ID_NB, REP_NUMBER ORDER BY STATUS, SRC_ACCOUNT_ID DESC) AS RNK1 
FROM EDH_STAGING.DIM_ACCOUNT_CA) AD1
WHERE RNK1 =1)  AD 
 ON
			T.BD_ID = AD.BD_ID
			AND T.TRIMMED_SPONS_ACCOUNT_NO = AD.TRIMMED_SPONS_ACCOUNT_NO
			AND UPPER(TRIM(T.REP_NUMBER_OF_RECORD)) = UPPER(TRIM(AD.REP_NUMBER ))
		LEFT JOIN EDH_STAGING.DIM_CLIENT_CA CD --splice-properties joinStrategy=SORTMERGE
 ON
			T.BD_ID = CD.BD_ID
			AND UPPER(TRIM(T.SSN)) = UPPER(TRIM(CD.TAX_ID_NB))
		LEFT JOIN EDH_STAGING.DIM_REP_NUMBER_CA DR --splice-properties joinStrategy=BROADCAST
 ON
			DR.BD_ID = T.BD_ID
			AND UPPER(TRIM(T.REP_NUMBER_OF_RECORD)) = UPPER(TRIM(DR.REP_NUMBER ))
		WHERE		NOT EXISTS(
				SELECT 1 FROM EDH_STAGING.TEMP_CA_TRADE_T2 T2
				WHERE
					T.TRADE_ID = T2.TRADE_ID
					AND T.BD_ID = T2.BD_ID
			)) T; 

	/* Collect stats and compaction on the tables*/
			
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('EDH_STAGING','TEMP_CA_TRADE_T2');

ANALYZE TABLE EDH_STAGING.TEMP_CA_TRADE_T2;

	/* Insert data from TEMP2 joined with REF table into Staging FACT CA */
	
INSERT INTO EDH_STAGING.FACT_PAID_TRADES_CA(
FACT_PAID_TRADES_KEY,
TRADE_DT,
PAID_DT,
BD_ID,
DIM_ACCOUNTS_KEY,
DIM_BRANCH_KEY,
DIM_REP_NUMBER_KEY,
DIM_CLIENT_KEY,
SPONS_ACCOUNT_NO,
TRIMMED_SPONS_ACCOUNT_NO,
CUSIP,
SRC_TRADE_ID,
REP_NUMBER_OF_RECORD,
TRANS_TYPE_CD,
DATA_SOURCE, 
LOB,         
SRC_LOB,
CLIENT_NM,
SPONSOR_NM,
PRODUCT_NM,
CANCEL_DT,
RECEIVE_DT,
SETTLEMENT_DT,
QUANTITY,
PRICE,
GDC_AMT,
REPORTED_GROSS_GDC_AMT,
SALES_AMT,
INVESTMENT_AMT,
TICKET_FEE_WAIVED_FLG,
INCOMMING_TICKET_FEE_AMT,
TICKET_FEE_AMT,
ELECTRONIC_TRADE_IND,
GDC_CREDIT_ONLY_IND,
BROKERAGE_FLG,
TRADE_CONFIRM_NUMBER,
DELETED_FLG,
LOAD_JOB_ID,
LOAD_TS,
TRADE_ID
) 
SELECT
PD.LAST_USED_TRADES_KEY +(ROW_NUMBER() OVER(ORDER BY T.PAID_DT ASC)) AS FACT_PAID_TRADES_KEY,
T.TRADE_DT,
T.PAID_DT,
T.BD_ID,
CASE
	WHEN T.DIM_ACCOUNTS_KEY IS NULL THEN '-2'
	ELSE DIM_ACCOUNTS_KEY
END AS DIM_ACCOUNTS_KEY,
CASE
	WHEN T.DIM_BRANCH_KEY IS NULL THEN '-2'
	ELSE DIM_BRANCH_KEY
END AS DIM_BRANCH_KEY,
CASE
	WHEN T.DIM_REP_NUMBER_KEY IS NULL THEN '-2'
	ELSE DIM_REP_NUMBER_KEY
END AS DIM_REP_NUMBER_KEY,
CASE
	WHEN T.DIM_CLIENT_KEY IS NULL THEN '-2'
	ELSE T.DIM_CLIENT_KEY
END AS DIM_CLIENT_KEY,
T.SPONS_ACCOUNT_NO,
T.TRIMMED_SPONS_ACCOUNT_NO,
T.CUSIP,
T.SRC_TRADE_ID,
T.REP_NUMBER_OF_RECORD,
T.TRANS_TYPE,
TRIM('BONUS') as DATA_SOURCE,
CASE
	WHEN T.LOB IS NULL THEN t.SRC_LOB
	ELSE T.LOB
END AS LOB,
CASE
	WHEN T.LOB IS NULL THEN t.SRC_LOB
	ELSE T.LOB
END AS SRC_LOB,
NULLIF(CASE WHEN Length(TRIM(COALESCE(CD.FIRST_NM,'') || COALESCE(' ' || CD.MIDDLE_NM,'') || COALESCE(' ' || CD.LAST_NM,''))) > 0 
THEN TRIM(COALESCE(CD.FIRST_NM,'') || COALESCE(' ' || CD.MIDDLE_NM,'') || COALESCE(' ' || CD.LAST_NM,''))
WHEN Length(TRIM(COALESCE(CD.FIRST_NM,'') || COALESCE(' ' || CD.MIDDLE_NM,'') || COALESCE(' ' || CD.LAST_NM,'')))=0
THEN T.CLIENT_NM ELSE 'ERROR' END,'') AS CD_CLIENT_NM,
R.FUND_FAMILY_NAME AS SPONSOR_NM,
T.PRODUCT_NM,
T.CANCEL_DT,
T.RECEIVE_DT,
T.SETTLEMENT_DT,
 COALESCE(T.QUANTITY,0) AS QUANTITY,
 COALESCE(T.PRICE,0) as PRICE,
 COALESCE(T.GDC_AMT,0) as GDC_AMT,
 COALESCE(T.REPORTED_GROSS_GDC_AMT,0) as REPORTED_GROSS_GDC_AMT,
 COALESCE(T.SALES_AMT,0) as SALES_AMT,
 COALESCE(T.INVESTMENT_AMT,0) AS INVESTMENT_AMT,
T.TICKET_FEE_WAIVED_FLG,
 COALESCE(T.INCOMMING_TICKET_FEE_AMT,0) as INCOMMING_TICKET_FEE_AMT, 
 COALESCE(T.TICKET_FEE_AMT,0) as TICKET_FEE_AMT,
T.ELECTRONIC_TRADE_IND,
T.GDC_CREDIT_ONLY_IND,
T.BROKERAGE_FLG,
T.TRADE_CONFIRM_NUMBER,
TRIM('N') AS DELETED_FLG,
TRIM('INITIAL_LOAD') AS LOAD_JOB_ID,
CURRENT_TIMESTAMP AS LOAD_TS,
T.TRADE_ID
		FROM
			EDH_STAGING.TEMP_CA_TRADE_T2 T --splice-properties useSpark=true
		LEFT JOIN REFERENCE_DATA.REF_PRODUCT_MASTER R ON
			UPPER(TRIM(T.CUSIP)) = UPPER(TRIM(R.CUSIP))
			INNER JOIN EDH_STAGING.TEMP_CA_MAX_PAID_DATE PD ON 
            PD.BD_PARTY_ID = T.BD_ID
		LEFT JOIN EDH_STAGING.DIM_CLIENT_CA CD ON
			CD.DIM_CLIENT_KEY = T.DIM_CLIENT_KEY
		WHERE
			RNK = 1;

	/* Step 5: Run Compaction and Analyze tables */
	
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('EDH_STAGING','FACT_PAID_TRADES_CA');

ANALYZE TABLE EDH_STAGING.FACT_PAID_TRADES_CA;
