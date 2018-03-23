#!/bin/bash

#############################################################################################






#############################################################################################
## Declaring variables and intializing log
timestamp=$( date '+%Y-%m-%d_%H%M%S' )
LOG_FILE="/home/splice/cetera/brokerage-operations/logs/brokerage_ops_master_sla_exp_${timestamp}.log"
exec &>>"${LOG_FILE}"

FILE_LOC="/home/splice/cetera/brokerage-operations/BROK_OPS_SLA_EXECEPTION_STG"

## Cleaning local processed file directory
echo "[============================== Cleaning local processed directory ==================================]"
rm -rf /home/splice/cetera/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/*
[[ $? -ne 0 ]] && echo "Error cleaning processed directory" && exit 1
echo ""; echo "Processed files dir successfully cleaned"; echo ""

echo ""
echo "Starting ETL process............"
for file in ${FILE_LOC}/*.csv
do
FILENAME=${file}
INPUT_FILE_WITH_ABSOLUTE_PATH=${FILENAME}
INPUT_FILE_NAME_ONLY=${INPUT_FILE_WITH_ABSOLUTE_PATH##\/*\/}
echo ""
echo "[====================================== Now processing ${INPUT_FILE_NAME_ONLY} ==================================]"
echo "Input File: ${INPUT_FILE_NAME_ONLY}"
INPUT_FILE_REC_CNT=$(wc -l ${FILENAME} | cut -d' ' -f1 )
echo "Input File Record Count: ${INPUT_FILE_REC_CNT}"
echo ""

REPORT_NAME=${INPUT_FILE_NAME_ONLY:0:7}
INPUT_FILE_LENGTH=${#INPUT_FILE_NAME_ONLY}
startindexdt=$(( INPUT_FILE_LENGTH - 12 ))
startindexyr=$(( INPUT_FILE_LENGTH - 6 ))
EXTRACT_DATE_PART=${INPUT_FILE_NAME_ONLY:$startindexdt:5}
EXTRACT_DATE_PART1=${EXTRACT_DATE_PART//_/-}
EXTRACT_YR_PPART=${INPUT_FILE_NAME_ONLY:$startindexyr:2}
MASTER_DATE="20${EXTRACT_YR_PPART}-${EXTRACT_DATE_PART1}"
timestamp=`date +%Y%m%d%H%M%S`
TEMP_FILE="/home/splice/cetera/brokerage-operations/tmp/TEMP_${timestamp}.tmp"
PROCESSED_FILE="/home/splice/cetera/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/PROCESSED_${REPORT_NAME}_${MASTER_DATE}_${timestamp}.csv"
cp ${file} ${TEMP_FILE}

while read line
do
 FIRST_FIELD=$( echo $line | cut -d'|' -f1 )
 FIRST_FIELD_WITHOUT_CR="${FIRST_FIELD//$'\r'/}"
 SECOND_FIELD=$( echo $line | cut -d'|' -f2 )
 SECOND_FIELD_WITHOUT_CR="${SECOND_FIELD//$'\r'/}"
 THIRD_FIELD=$( echo $line | cut -d'|' -f3 )
 THIRD_FIELD_WITHOUT_CR="${THIRD_FIELD//$'\r'/}"
 FOURTH_FIELD=$( echo $line | cut -d'|' -f4 )
 FOURTH_FIELD_WITHOUT_CR="${FOURTH_FIELD//$'\r'/}"
 FIFTH_FIELD=$( echo $line | cut -d'|' -f5 )
 FIFTH_FIELD_WITHOUT_CR="${FIFTH_FIELD//$'\r'/}"
 SIXTH_FIELD=$( echo $line | cut -d'|' -f6 )
 SIXTH_FIELD_WITHOUT_CR="${SIXTH_FIELD//$'\r'/}"
 SEVENTH_FIELD=$( echo $line | cut -d'|' -f7 )
 SEVENTH_FIELD_WITHOUT_CR="${SEVENTH_FIELD//$'\r'/}"
# EIGHTH_FIELD=$( echo $line | cut -d'|' -f8 )
# EIGHTH_FIELD_WITHOUT_CR="${EIGHTH_FIELD//$'\r'/}"
 nine1=$( echo $line | cut -d'|' -f8 )
 nine=${nine1:-"31-DEC-2261 11:59:59"}
 NINTH_FIELD_WITH_DT_FORMAT=$( date -d"${nine}" '+%Y-%m-%d %H:%M:%S' )
 NINTH_FIELD_WITH_DT_FORMAT_WITHOUT_CR="${NINTH_FIELD_WITH_DT_FORMAT//$'\r'/}"
 tenth1=$( echo $line | cut -d'|' -f9 )
 tenth=${tenth1:-"31-DEC-2261 11:59:59"}
 TENTH_FIELD_WITH_DT_FORMAT_WITHOUT_CR="${tenth//$'\r\n'/}"
 TENTH_FIELD_WITH_DT_FORMAT=$( date -d"${TENTH_FIELD_WITH_DT_FORMAT_WITHOUT_CR}" '+%Y-%m-%d %H:%M:%S' )
# TENTH_FIELD_WITH_DT_FORMAT_WITHOUT_CR="${TENTH_FIELD_WITH_DT_FORMAT//$'\r'/}"
 
 echo "${MASTER_DATE}|${FIRST_FIELD_WITHOUT_CR}|${SECOND_FIELD_WITHOUT_CR}|${THIRD_FIELD_WITHOUT_CR}|${FOURTH_FIELD_WITHOUT_CR}|${FIFTH_FIELD_WITHOUT_CR}|${SIXTH_FIELD_WITHOUT_CR}|${SEVENTH_FIELD_WITHOUT_CR}|${NINTH_FIELD_WITH_DT_FORMAT_WITHOUT_CR}|${TENTH_FIELD_WITH_DT_FORMAT}" >> ${PROCESSED_FILE}
done < ${TEMP_FILE}

## Reporting processed file
PROCESSED_FILE_REC_CNT=$(wc -l ${PROCESSED_FILE} |  cut -d' ' -f1 )
echo "Processed File: PROCESSED_${REPORT_NAME}_${MASTER_DATE}_${timestamp}.csv"
echo "Processed File Record Count: ${PROCESSED_FILE_REC_CNT}"
rm -f ${TEMP_FILE}
done

HDFS_FILE_CNT=$(hdfs dfs -count /data/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED | awk -F '[[:space:]]+' '{print $3}')
if [[ ${HDFS_FILE_CNT} -ne 0 ]]
then

## Cleaning PRD HDFS processed file directory
echo "[=============================== Archiving HDFS directory =================================]"
hdfs dfs -rm -skipTrash /data/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && exit 1
echo ""; echo "Older HDFS files archived sucessfully..."; echo ""
fi

## Moving processed file to HDFS directory
echo "[=============================== Moving PRD processed files to HDFS directory =============================]"
CAL_TO_HDFS=$(hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/* /data/brokerage-operations/BROK_OPS_SLA_EXCEPTION_PROCESSED/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && exit 1
echo ""; echo "Processed files successfully moved to HDFS dir"; echo ""

exit 0
