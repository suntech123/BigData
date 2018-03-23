#!/bin/bash

#############################################################################################################################






#############################################################################################################################

## Declaring variables and intializing log
timestamp=$( date '+%Y-%m-%d_%H%M%S' )
LOG_FILE="/home/splice/cetera/brokerage-operations/logs/brokerage_ops_master_sla_${timestamp}.log"
exec &>>"${LOG_FILE}"

## Assigning Configuration Directories.
INPUT_FILES_LANDING_DIR="/home/splice/cetera/brokerage-operations/BROK_OPS_SLA_STG"
[[ -d ${INPUT_FILES_LANDING_DIR} ]] && cd "${INPUT_FILES_LANDING_DIR}"
LKP_DIR="/home/splice/cetera/brokerage-operations/BROK_OPS_LKP_FILE_DIR"

## Cleaning local processed file directory
echo "[============================== Cleaning local processed directory ==================================]"
rm -rf /home/splice/cetera/brokerage-operations/BROK_OPS_SLA_PROCESSED/*
[[ $? -ne 0 ]] && echo "Error cleaning processed directory" && exit 1
echo ""; echo "Processed files dir successfully cleaned"; echo ""

## Checking source directory file count
echo "[========================= Checking File statistics ============================]"; echo ""
SRC_CNT=$( find ${INPUT_FILES_LANDING_DIR} -name '*.csv' | wc -l )
[[ ${SRC_CNT} -ne 5 ]] && echo "File count mismatch....exiting" && exit 1
echo "Source file count in staging dir BROK_OPS_PRD_STG : ${SRC_CNT}"

## Remove all spaces from file names
for file in *
do
RAW_INPUT_FILE="$file"
MOD_INPUT_FILE="${RAW_INPUT_FILE// /_}"
[[ "${RAW_INPUT_FILE}" != "${MOD_INPUT_FILE}" ]] && mv "${RAW_INPUT_FILE}" "${MOD_INPUT_FILE}" 2>&1
done

##change the delimiter for each file
##sed -i 's/,/|/g' ${INPUT_FILES_LANDING_DIR}/*.csv

## ETL cycle to process all files in Landing Dir
echo "[============================= Starting ETL cycle ==============================================]"; echo ""
for fhandle in *.csv
do
FILENAME=${fhandle}
echo ""
echo "[============================= Now processing file : ${FILENAME} ===============================]"; echo ""
echo "Input File: ${FILENAME}"
INPUT_FILE_REC_CNT=$(wc -l ${FILENAME} | cut -d' ' -f1 )
echo "Input File Record Count: ${INPUT_FILE_REC_CNT}"
echo ""
INPUT_FILE_WITH_ABSOLUTE_PATH=${FILENAME}
INPUT_FILE_NAME_ONLY=${INPUT_FILE_WITH_ABSOLUTE_PATH##\/*\/}
REPORT_NAME=${INPUT_FILE_NAME_ONLY:0:7}
INPUT_FILE_LENGTH=${#INPUT_FILE_NAME_ONLY}
startindexdt=$(( INPUT_FILE_LENGTH - 12 ))
startindexyr=$(( INPUT_FILE_LENGTH - 6 ))
EXTRACT_DATE_PART=${INPUT_FILE_NAME_ONLY:$startindexdt:5}
EXTRACT_DATE_PART1=${EXTRACT_DATE_PART//_/-}
EXTRACT_YR_PPART=${INPUT_FILE_NAME_ONLY:$startindexyr:2}

##Getting the report name from the file name
case "${INPUT_FILE_LENGTH}" in
21) REPORT_NAME=${INPUT_FILE_NAME_ONLY:0:8}
    ;;
23) REPORT_NAME=${INPUT_FILE_NAME_ONLY:0:10}
    ;;
*)  ;;
esac

stamp=`date +%Y%m%d%H%M%S`
ORIG_FILE_TEMP="/home/splice/cetera/brokerage-operations/tmp/ORIG_TEMP_${stamp}.tmp"
cp ${INPUT_FILES_LANDING_DIR}/${FILENAME} ${ORIG_FILE_TEMP}

##Getting the team name from report name
case "${REPORT_NAME}" in
RPT_08.0) TEAM="100"
          HEADER_STR="HEADER_8.0="
          HEADER_REC=$(awk "BEGIN{IGNORECASE=1};/^${HEADER_STR}/" ${LKP_DIR}/header_detail_sla.csv | cut -d'=' -f2)
          sed -i "1i ${HEADER_REC}" ${ORIG_FILE_TEMP} 2>&1
          ;;
RPT_08.1) TEAM="101"
          HEADER_STR="HEADER_8.1="
          HEADER_REC=$(awk "BEGIN{IGNORECASE=1};/^${HEADER_STR}/" ${LKP_DIR}/header_detail_sla.csv | cut -d'=' -f2)
          sed -i "1i ${HEADER_REC}" ${ORIG_FILE_TEMP} 2>&1
          ;;
#RPT_8.4) TEAM="102"
#          HEADER_STR="HEADER_8.4="
#          HEADER_REC=$(awk "BEGIN{IGNORECASE=1};/^${HEADER_STR}/" ${LKP_DIR}/header_detail_sla.csv | cut -d'=' -f2)
#          sed -i "1i ${HEADER_REC}" ${ORIG_FILE_TEMP} 2>&1
#          ;;
RPT_08.2.1) TEAM="102"
          HEADER_STR="HEADER_8.2.1="
          HEADER_REC=$(awk "BEGIN{IGNORECASE=1};/^${HEADER_STR}/" ${LKP_DIR}/header_detail_sla.csv | cut -d'=' -f2)
          sed -i "1i ${HEADER_REC}" ${ORIG_FILE_TEMP} 2>&1
          ;;
RPT_08.3.1) TEAM="103"
            HEADER_STR="HEADER_8.3.1="
            HEADER_REC=$(awk "BEGIN{IGNORECASE=1};/^${HEADER_STR}/" ${LKP_DIR}/header_detail_sla.csv | cut -d'=' -f2)
            sed -i "1i ${HEADER_REC}" ${ORIG_FILE_TEMP} 2>&1
            ;;
RPT_08.5.1) TEAM="106"
          HEADER_STR="HEADER_8.5.1="
          HEADER_REC=$(awk "BEGIN{IGNORECASE=1};/^${HEADER_STR}/" ${LKP_DIR}/header_detail_sla.csv | cut -d'=' -f2)
          sed -i "1i ${HEADER_REC}" ${ORIG_FILE_TEMP} 2>&1
          ;;
*) ;;
esac

MASTER_DATE="20${EXTRACT_YR_PPART}-${EXTRACT_DATE_PART1}"
#echo ${MASTER_DATE}
#MASTER_HEADER="ACTIVITY_DATE|USER_KEY|ACTIVITY|ACTIVITY_NAME|ACTIVITY_STATUS|ACTIVITY_COUNT"

timestamp=`date +%Y%m%d%H%M%S`
TEMP_FILE="/home/splice/cetera/brokerage-operations/tmp/TEMP_${timestamp}.tmp"
PROCESSED_FILE="/home/splice/cetera/brokerage-operations/BROK_OPS_SLA_PROCESSED/PROCESSED_${REPORT_NAME}_${MASTER_DATE}_${timestamp}.csv"
HEADER=${HEADER_REC}
cp ${ORIG_FILE_TEMP} ${TEMP_FILE}
sed -i '1d' ${TEMP_FILE}

####awk "/^Build Advisory Reject/" activity_desc.csv

## Count no of fields in file 

TOTAL_COLS=$(head -1 ${TEMP_FILE} | sed 's/[^|]//g' | wc -c )
##INNER_LOOP_COUNT=$(( TOTAL_COLS - 2 ))
######echo ${INNER_LOOP_COUNT}

## Read file line by line and process it
while read line
do 
    FIRST_FIELD=$( echo $line | cut -d'|' -f1 )
    SECOND_FIELD=$( echo $line | cut -d'|' -f2 )
    THIRD_FIELD=$( echo $line | cut -d'|' -f3 )
    FOURTH_FIELD=$( echo $line | cut -d'|' -f4 )
    FIFTH_FIELD=$( echo $line | cut -d'|' -f5 )
    SIXTH_FIELD=$( echo $line | cut -d'|' -f6 )
    SEVENTH_FIELD=$( echo $line | cut -d'|' -f7 )
    NINTH_FIELD=$( echo $line | cut -d'|' -f13 )
    NINTH_FIELD_WITH_DT_FORMAT=$( date -d"${NINTH_FIELD}" '+%Y-%m-%d %H:%M:%S' )
    TENTH_FIELD=$( echo $line | cut -d'|' -f14 )
    TENTH_FIELD_WITH_DT_FORMAT=$( date -d"${TENTH_FIELD}" '+%Y-%m-%d %H:%M:%S' )
    ITM_CMPLTD_WITHIN_SLA=$( echo $line | cut -d'|' -f15 )
    for (( i=0; i<5; i++ ))
     do
      NEXT_COL=$( echo "$HEADER" | cut -d'|' -f$(( i+8 )) )
      ACTIVITY_COUNT=$( echo "$line" | cut -d'|' -f$(( i+8 )) )
          ## remove carriage return
          NEXT_COL_WITHOUT_CR="${NEXT_COL//$'\r'/}"
#          NEXT_THREE_FIELDS=$( awk "BEGIN{IGNORECASE=1};/^${NEXT_COL_WITHOUT_CR}\|/" activity_desc.csv )
#          NEXT_THREE_FIELDS_WITHOUT_NL="${NEXT_THREE_FIELDS//$'\r'/}"
          ACTIVITY_COUNT_WITHOUT_CR="${ACTIVITY_COUNT//$'\r'/}"
          ITM_CMPLTD_WITHIN_SLA_WITOUT_CR="${ITM_CMPLTD_WITHIN_SLA//$'\r'/}"
      echo "${MASTER_DATE}|${TEAM}|${FIRST_FIELD}|${SECOND_FIELD}|${THIRD_FIELD}|${SEVENTH_FIELD}|${FOURTH_FIELD}|${FIFTH_FIELD}|${NEXT_COL_WITHOUT_CR}|${ACTIVITY_COUNT_WITHOUT_CR}|${SIXTH_FIELD}|${NINTH_FIELD_WITH_DT_FORMAT}|${TENTH_FIELD_WITH_DT_FORMAT}|${ITM_CMPLTD_WITHIN_SLA_WITOUT_CR:--NA-}" >> ${PROCESSED_FILE} 
     done
done < ${TEMP_FILE}
PROCESSED_FILE_REC_CNT=$(wc -l ${PROCESSED_FILE} |  cut -d' ' -f1 )
echo "Processed File: PROCESSED_${REPORT_NAME}_${MASTER_DATE}_${timestamp}.csv"
echo "Processed File Record Count: ${PROCESSED_FILE_REC_CNT}"
rm -f ${ORIG_FILE_TEMP}
rm -f ${TEMP_FILE}
done

HDFS_FILE_CNT=$(hdfs dfs -count /data/brokerage-operations/BROK_OPS_SLA_PROCESSED | awk -F '[[:space:]]+' '{print $3}')
if [[ ${HDFS_FILE_CNT} -ne 0 ]]
then
## Cleaning SLA HDFS processed file directory
echo "[=============================== Archiving HDFS directory =================================]"
hdfs dfs -rm -skipTrash /data/brokerage-operations/BROK_OPS_SLA_PROCESSED/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && exit 1
echo ""; echo "Older HDFS files archived sucessfully..."; echo ""
fi

## Moving processed file to HDFS directory
echo "[=============================== Moving PRD processed files to HDFS directory =============================]"
CAL_TO_HDFS=$(hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations/BROK_OPS_SLA_PROCESSED/* /data/brokerage-operations/BROK_OPS_SLA_PROCESSED/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && exit 1
echo ""; echo "Processed files successfully moved to HDFS dir"; echo ""

exit 0
