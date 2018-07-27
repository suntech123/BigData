/*
##################################################################################################################
Purpose: 
		1. To consolidate data from taccountinfo from PBD_SMARTWORKS. CLIENT_ACCOUNT_DATE table
		2. To consolidate data from CD_CLIENT, taccountinfo from PBD_SMARTWORKS. TEMP_CA_CD_CLIENT tables
		3. Create a Stage Tables that captures all the client information for BD CA
		4. Load the data to consolidate EDH Base table - EDH.DIM_CLIENT_ADDRESS

##################################################################################################################
*/
elapsedtime ON;

/* Step 1: Truncate staging table DIM_CLIENT_ADDRESS_CA */
TRUNCATE TABLE
	EDH_STAGING.DIM_CLIENT_ADDRESS_CA;

/* Step 2: Insert client_address information from PBD_SMARTWORKS */
INSERT
	INTO
		EDH_STAGING.DIM_CLIENT_ADDRESS_CA(
			DIM_CLIENT_ADDRESS_KEY,
			DIM_CLIENT_KEY,
			BD_ID,
			SRC_CLIENT_ID,
			ADDRESS_TYPE,
			ADDRESS_LINE1,
			ADDRESS_LINE2,
			CITY,
			STATE,
			ZIP,
			COUNTRY,
			PRIMARY_FLG,
			CURRENT_FLG,
			DELETED_FLG,
			EFF_START_DT,
			EFF_END_DT,
			LOAD_JOB_ID,
			LOAD_TS,
			LST_UPDT_JOB_ID,
			LST_UPDT_TS
		) SELECT
			DIM_ADDRESS_CLIENT_KEY,
			DIM_CLIENT_KEY,
			BD_ID,
			SRC_CLIENT_ID,
			ADDRESS_TYPE,
			ADDRESS_LINE1,
			ADDRESS_LINE2,
			CITY,
			STATE,
			ZIP,
			COUNTRY_CD,
			PRIMARY_FLG,
			CURRENT_FLG,
			DELETE_FLG,
			EFF_START_DT,
			EFF_END_DT,
			CREATED_BY,
			CREATE_DT,
			UPDATED_BY,
			UPDATED_DT
		FROM
			(
				SELECT
					DISTINCT CONCAT( DC.DIM_CLIENT_KEY, TRIM( CAST( CA.ADDRESS_ID AS CHAR( 30 )))) AS DIM_ADDRESS_CLIENT_KEY,
					DC.DIM_CLIENT_KEY,
					DC.BD_ID,
					DC.SRC_CLIENT_ID,
					CA.ADDRESS_TYPE,
					a.ADDRESS_LINE1,
					A.ADDRESS_LINE2,
					A.CITY,
					A.STATE,
					A.ZIP,
					A.COUNTRY_CD,
					CASE
						WHEN CA.ADDRESS_TYPE = 'MAIL' THEN 'Y'
						ELSE 'N'
					END AS PRIMARY_FLG,
					'Y' AS current_flg,
					'N' AS DELETE_flg,
					DC.eff_Start_Dt,
					CAST(
						'2261-12-31' AS DATE
					) AS EFF_END_DT,
					'INITIAL_LOAD' AS CREATED_BY,
					CASE
						WHEN DC.LOAD_TS > CA.INSERT_LOAD_TS THEN DC.LOAD_TS
						ELSE CA.INSERT_LOAD_TS
					END AS create_dt,
					'INITIAL_LOAD' AS updated_BY,
					CASE
						WHEN DC.LST_UPDT_TS > COALESCE(
							ca.LST_UPDATE_TS,
							ca.INSERT_LOAD_TS
						) THEN DC.LST_UPDT_TS
						ELSE COALESCE(
							ca.LST_UPDATE_TS,
							ca.INSERT_LOAD_TS
						)
					END AS updated_DT,
					ROW_NUMBER() OVER(
						PARTITION BY dc.DC.DIM_CLIENT_KEY,
						ca.ADDRESS_TYPE
					ORDER BY
						ca.MODIFIED_DATE DESC,
						ca.ADDRESS_ID DESC
					) RNKC
				FROM
					EDH_STAGING.DIM_CLIENT_CA DC --splice-properties useSpark=true
				INNER JOIN EDH_STAGING.XREF_CLIENT_CA XC ON
					DC.BD_ID = XC.BD_ID
					AND DC.TAX_ID_NB = XC.TAX_ID_NB
				INNER JOIN EDH_STAGING.TEMP_CA_CD_CLIENT_ADDRESS CA ON
					XC.SRC_CLIENT_ID = CLIENT_ID_VARCHAR
				INNER JOIN PBD_SMARTWORKS.CD_ADDRESS A ON
					CA.ADDRESS_ID = A.ADDRESS_ID
				WHERE
					DC.BD_ID = 4
					AND CA.RNK = 1
			) a
		WHERE
			RNKC = 1;

/* Step 3: Run Compaction and Analyze tables */
CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(
	'EDH_STAGING',
	'DIM_CLIENT_ADDRESS_CA'
);

ANALYZE TABLE
	EDH_STAGING.DIM_CLIENT_ADDRESS_CA;
