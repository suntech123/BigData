#!/bin/bash
#################################################################################################
#                                                                                               #
#                                                                                               #
#                                                                                               #
#################################################################################################

## Declaring variables and intializing log
timestamp=$( date '+%Y-%m-%d_%H%M%S' )
LOG_FILE="/home/splice/cetera/brokerage-operations-quality-center/logs/brokerage_ops_quality_center_file_watcher_${timestamp}.log"
exec &>>"${LOG_FILE}"

LIN_LND_DIR="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_QC_LND"
LIN_LND_DIR_TMP="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_QC_LND_TEMP"
WORKFLOWQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_WORKFLOWQC_STG"
VELOCITYQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_VELOCITYQC_STG"
ISSUELOGQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_ISSUELOGQC_STG"
DIRECTBUSINESSQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_DIRECTBUSINESSQC_STG"
DOCUPACEQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_DOCUPACEQC_STG"
OVERSIGHTLOGQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_OVERSIGHTLOGQC_STG"
PHONEQC_STG="/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_PHONEQC_STG"

## Clean Landing Directory before further processing
echo "Cleaning the linux landing directory ........"
rm -f ${LIN_LND_DIR}/*

## Clean all the landing directories before further processing...
echo ""
echo "Cleaning the STG directories...."
rm -f ${WORKFLOWQC_STG}/*
rm -f ${VELOCITYQC_STG}/*
rm -f ${ISSUELOGQC_STG}/*
rm -f ${DIRECTBUSINESSQC_STG}/*
rm -f ${DOCUPACEQC_STG}/*
rm -f ${OVERSIGHTLOGQC_STG}/*
rm -f ${PHONEQC_STG}/*

## Copy all files to Linux Landing directory
echo ""
echo "Copying files from network source directory to linux landing dir..." ; echo ""
src_cnt=$(ls -fq /home/splice/loadFile/data/QualityCenterData/ | wc -l)
src_file_cnt=$(( ${src_cnt} -2 ))
#[[ ${src_file_cnt} -eq 0 ]] && echo "Source folder is empty...exiting without copy" && exit 1
cp /home/splice/loadFile/data/QualityCenterData/* ${LIN_LND_DIR}/
echo ""
[[ $? -ne 0 ]] && echo "Something went wrong while copying...exiting script.." && exit 1
echo "Files copied successfully"

## Remove any space or dash characters from file names.
for file in ${LIN_LND_DIR}/*
do
RAW_INPUT_FILE="$file"
MOD_INPUT_FILE="${RAW_INPUT_FILE// /_}"
##MOD_INPUT_FILE1="${MOD_INPUT_FILE//RPT_8/RPT_08}"
[[ "${RAW_INPUT_FILE}" != "${MOD_INPUT_FILE}" ]] && mv "${RAW_INPUT_FILE}" "${MOD_INPUT_FILE}" 2>&1
done

## Remove non printable character and registered trademark symbol
for file in ${LIN_LND_DIR}/*.csv
do
/home/splice/cetera/brokerage-operations-quality-center/remove_spl_chars.py ${file}
done

## Clean Landing Directory before further processing
echo "Cleaning the linux landing directory second time for copying cleaned files........"
rm -f ${LIN_LND_DIR}/*

## Copying cleaned files from temp landing back to landing directory
echo "Moving the cleaned files from temp landing to landing directory...."
mv ${LIN_LND_DIR_TMP}/*.csv ${LIN_LND_DIR}/

## Change the delimiter of the files from comma to pipe except double quoted filelds
#perl -i -pe 's/Â® |â€™//g' ${LIN_LND_DIR}/*.csv

sed -i "s/[']/=/g" ${LIN_LND_DIR}/*.csv
##sed -i ':a;s/^\(\(\(['\''"]\)[^\3]*\3\|[^",'\'']*\)*\),/\1|/;ta' ${LIN_LND_DIR}/*.csv ---Not fully reliable

## Remove unicode characters specially in issuelogqc
sed -i 's/[%]//g' ${LIN_LND_DIR}/*.csv

sed -i ':a;s/^\(\("[^"]*"\|'\''[^'\'']*'\''\|[^",'\'']*\)*\),/\1|/;ta' ${LIN_LND_DIR}/*.csv
sed -i 's/\"//g' ${LIN_LND_DIR}/*.csv
sed -i "s/[=]/'/g" ${LIN_LND_DIR}/*.csv

## Delete the header line from each file
sed -i '1d' ${LIN_LND_DIR}/*.csv

## Delete empty lines if any from all files
sed -i '/^$/d' ${LIN_LND_DIR}/*.csv

## Move the files to seperate staging directories for PRD and SLA
echo ""
echo "Moving files from linux landing directory to respective staging dirs......." ; echo ""
mv ${LIN_LND_DIR}/DirectBusinessQC*.csv ${DIRECTBUSINESSQC_STG}/
[[ $? -ne 0 ]] && echo "DIRECTBUSINESSQC_STG copy failed" && echo ""
# Delete all the null fields
sed -i '/||||||||||/d' ${DIRECTBUSINESSQC_STG}/*.csv
mv ${LIN_LND_DIR}/DocupaceQC* ${DOCUPACEQC_STG}/
[[ $? -ne 0 ]] && echo "DOCUPACEQC_STG copy failed" && echo ""
sed -i '/||||||||||||||||/d' ${DOCUPACEQC_STG}/*.csv
mv ${LIN_LND_DIR}/IssuesLogQC* ${ISSUELOGQC_STG}/
[[ $? -ne 0 ]] && echo "ISSUELOGQC_STG copy failed" && echo ""
sed -i '/|||||||||||||||||||/d' ${ISSUELOGQC_STG}/*.csv
mv ${LIN_LND_DIR}/OversightLogQC* ${OVERSIGHTLOGQC_STG}/
[[ $? -ne 0 ]] && echo "OVERSIGHTLOGQC_STG copy failed" && echo ""
sed -i '/|||||||||||||/d' ${OVERSIGHTLOGQC_STG}/*.csv
mv ${LIN_LND_DIR}/PhoneQC* ${PHONEQC_STG}/
[[ $? -ne 0 ]] && echo "PHONEQC_STG copy failed"
sed -i '/||||/d' ${PHONEQC_STG}/*.csv
###sed -i '1,16d' ${PHONEQC_STG}/*.csv
mv ${LIN_LND_DIR}/VelocityQC* ${VELOCITYQC_STG}/
[[ $? -ne 0 ]] && echo "VELOCITYQC_STG copy failed" && echo ""
sed -i '/||||||||||/d' ${VELOCITYQC_STG}/*.csv
mv ${LIN_LND_DIR}/WorkflowQC* ${WORKFLOWQC_STG}/
[[ $? -ne 0 ]] && echo "WORKFLOWQC_STG copy failed" && echo ""
sed -i '/||||||||||/d' ${WORKFLOWQC_STG}/*.csv
echo "Files moved successfully..........."

## Cleaning PRD HDFS processed file directory
echo "[=============================== Archiving HDFS directory =================================]"
sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/WORKFLOWQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && echo ""

sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/VELOCITYQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && echo ""

sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/ISSUELOGQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && echo ""

sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/DIRECTBUSINESSQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && echo ""

sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/DOCUPACEQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && echo ""

sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/OVERSIGHTLOGQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" && echo ""

sudo -su hdfs hdfs dfs -rm -skipTrash /data/brokerage-operations-quality-center/PHONEQC_STG/* > /dev/null
[[ $? -ne 0 ]] && echo "Older HDFS files could not be archived" 

echo ""; echo "Older HDFS files archived sucessfully..."; echo ""


## Moving processed file to HDFS directory
echo "[=============================== Moving PRD processed files to HDFS directory =============================]"

CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_WORKFLOWQC_STG/* /data/brokerage-operations-quality-center/WORKFLOWQC_STG/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""
CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_VELOCITYQC_STG/* /data/brokerage-operations-quality-center/VELOCITYQC_STG/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""
CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_ISSUELOGQC_STG/* /data/brokerage-operations-quality-center/ISSUELOGQC_STG > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""
CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_DIRECTBUSINESSQC_STG/* /data/brokerage-operations-quality-center/DIRECTBUSINESSQC_STG/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""
CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_DOCUPACEQC_STG/* /data/brokerage-operations-quality-center/DOCUPACEQC_STG/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""
CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_OVERSIGHTLOGQC_STG/* /data/brokerage-operations-quality-center/OVERSIGHTLOGQC_STG/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""
CAL_TO_HDFS=$(sudo -su hdfs hadoop fs -copyFromLocal /home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_PHONEQC_STG/* /data/brokerage-operations-quality-center/PHONEQC_STG/ > /dev/null)
[[ $? -ne 0 ]] && echo "File could not be copied" && echo ""

echo ""; echo "Processed files successfully moved to HDFS dir"; echo ""

## Moving the files from daily folder to archive folder on network drive
echo ""
echo "Archiving files on the network drive" ; echo ""
mv /home/splice/loadFile/data/QualityCenterData/* /home/splice/loadFile/data/QualityCenterDataArchive/

exit 0
