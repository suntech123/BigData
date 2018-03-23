#!/bin/bash
#################################################################################################
#												#
#												#
#												#
#################################################################################################

## Declaring variables and intializing log
timestamp=$( date '+%Y-%m-%d_%H%M%S' )
LOG_FILE="/home/splice/cetera/brokerage-operations/logs/brokerage_ops_file_watcher_${timestamp}.log"
exec &>>"${LOG_FILE}"
LIN_LND_DIR="/home/splice/cetera/brokerage-operations/BROK_OPS_LND"
PRD_STG="/home/splice/cetera/brokerage-operations/BROK_OPS_PRD_STG"
SLA_STG="/home/splice/cetera/brokerage-operations/BROK_OPS_SLA_STG"
SLA_EXCEPTION_STG="/home/splice/cetera/brokerage-operations/BROK_OPS_SLA_EXECEPTION_STG"

## Clean Landing Directory before further processing
echo "Cleaning the linux landing directory ........"
rm -f ${LIN_LND_DIR}/*

## Clean stage Directory
echo "Cleaning the linux stg directory ........"
rm -f ${PRD_STG}/*
rm -f ${SLA_STG}/*
rm -f ${SLA_EXCEPTION_STG}/*


## Copy all files to Linux Landing directory
echo ""
echo "Copying files from network source directora to linux landing dir..." ; echo ""
src_cnt=$(ls -fq /home/splice/loadFile/data/BrokerageOpsDaily/ | wc -l)
src_file_cnt=$(( ${src_cnt} -2 ))

echo $src_file_cnt

[[ ${src_file_cnt} -eq 0 ]] && echo "Source folder is empty...exiting without copy" && exit 1
cp /home/splice/loadFile/data/BrokerageOpsDaily/* ${LIN_LND_DIR}/
echo ""
[[ $? -ne 0 ]] && echo "Something went wrong while copying...exiting script.." && exit 1
echo "Files copied successfully"

## Get the date of the files from filename
FNAME_FULL_PATH=$( find ${LIN_LND_DIR} -name 'RPT_11.0*' )
FNAME=${FNAME_FULL_PATH##\/*\/}
FDATE=${FNAME:9:8}

## Check the count of the files in landing directory.
echo ""
echo -n  "Validating file count in linux landing directory...."
x=$(ls -fq "${LIN_LND_DIR}" | wc -l)
FILE_CNT=$(( x - 2 ))
[[ ${FILE_CNT} -ne 13 ]] && echo "File Count doesn't match" && exit 1
echo "Count Matches"

## Remove any space or dash characters from file names.
for file in ${LIN_LND_DIR}/*
do
RAW_INPUT_FILE="$file"
MOD_INPUT_FILE="${RAW_INPUT_FILE// /_}"
MOD_INPUT_FILE1="${MOD_INPUT_FILE//RPT_8/RPT_08}"
[[ "${RAW_INPUT_FILE}" != "${MOD_INPUT_FILE1}" ]] && mv "${RAW_INPUT_FILE}" "${MOD_INPUT_FILE1}" 2>&1
done

## Change the delimiter of the files from comma to pipe except double quoted filelds
sed -i "s/[']/=/g" ${LIN_LND_DIR}/*.csv
##sed -i ':a;s/^\(\(\(['\''"]\)[^\3]*\3\|[^",'\'']*\)*\),/\1|/;ta' ${LIN_LND_DIR}/*.csv ---Not fully reliable

sed -i ':a;s/^\(\("[^"]*"\|'\''[^'\'']*'\''\|[^",'\'']*\)*\),/\1|/;ta' ${LIN_LND_DIR}/*.csv
sed -i 's/\"//g' ${LIN_LND_DIR}/*.csv
sed -i "s/[=]/'/g" ${LIN_LND_DIR}/*.csv

## sed -i 's/,/|/g' ${INPUT_FILES_LANDING_DIR}/*.csv

## Check if the file naming convention correct or not.
##FL_CNT=$(find ${LIN_LND_DIR} -type f -name "*.csv" | wc -l)
##[[ ${FL_CNT} -ne 13 ]] && echo "There is some problem in file extention" && exit 1

## Move the files to seperate staging directories for PRD and SLA
echo ""
echo "Moving files from linux landing directory to respective staging dirs......." ; echo ""
mv ${LIN_LND_DIR}/RPT_11* ${PRD_STG}/
[[ $? -ne 0 ]] && echo "PRD copy failed" && exit 1
mv ${LIN_LND_DIR}/RPT_08* ${SLA_STG}/
[[ $? -ne 0 ]] && echo "SLA copy failed" && exit 1
mv ${SLA_STG}/RPT_08.4* ${SLA_EXCEPTION_STG}/
[[ $? -ne 0 ]] && echo "SLA exception failed" && exit 1
echo "Files moved successfully..........."

## Moving the files from daily folder to archive folder on network drive
echo ""
echo "Archiving files on the network drive" ; echo ""
mkdir -p /home/splice/loadFile/data/BrokerageOpsArchive/RPT_${FDATE}
mv /home/splice/loadFile/data/BrokerageOpsDaily/* /home/splice/loadFile/data/BrokerageOpsArchive/RPT_${FDATE}/
exit 0
