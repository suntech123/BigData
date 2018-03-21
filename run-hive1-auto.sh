#!/bin/bash

#Purpose: The script will read a file containing a list of tables, one table per row 
#	  in the format of SCHEMA.TABLE_NAME.COLUMN. The COLUMN is an optional entry.
#	  If a COLUMN is specified sqoop will use that column to split the extract
# 	  among the mappers using that column. It then calls a sqoop job to extract the
#	  data from the database and put the data in HDFS. Lastly, a script will be executed
#	  to import the data into Splice Machine.
#
#	  Parameter:
#		1. Sqoop Configuration File - File containing the sqoop parameters
#		2. Table List File - File containing list of table(s) in the format of SCHEMA.TABLE_NAME.COLUMN:MAPPERS
#			where COLUMN and MAPPERS are optional
#		3. Splice Schema Name - The name of the schema in Splice to import to
#		4. Import File Path - The path to the directory containing the import sql statements.
#					Each table being imported must have an associated file in this directory
#					named in the format import-<schema>-<table>.sql. The file names are case
#					sensitive.
#
# Usage: ./run-sqoop-full.sh <config file> <table list file> <splice schema name> <import file path> <query file path> <log file name>

SQOOP_CONFIG_FILE=$1
FILE_LIST=$2
SPLICE_SCHEMA=$3
IMPORT_PATH=$4
UPSERT_PATH=$5
QUERY_PATH=$6
LOG_FILE=$7

SQLSERVER_DBO="dbo"
SQLSERVER_DBOC="DBO"
COLON=":"
DEFAULT_MAPPERS="8"

JOB_RUN_DATE=$(date +"%Y-%m-%d %T.0")
JOB_RUN_DATE1=$(date +"%Y-%m-%d-%T")
jobstartdate=$(date +"%Y-%m-%d %T.0")

# check for correct number of parameters
if [ $# -eq 7 ]; then
	# check if the sqoop config file exists
	if [ ! -f $SQOOP_CONFIG_FILE ]; then
		echo $SQOOP_CONFIG_FILE not found
		exit 1
	fi
	
	# check if the file list exists
	if [ ! -f $FILE_LIST ]; then
		echo $FILE_LIST not found
		exit 1
	else
		cp $FILE_LIST $FILE_LIST-FAILED
		# loop through th file list and parse each line	
	   cat $FILE_LIST | while IFS=. read schema_id table_id splice_tablename frequency schema loadtype sourcetype hive_diff hive_cdc sqooprequired table column
		do {
			echo Exporting $schema.$table
			loadtype1=$loadtype
			if [ $column ]; then
				echo Splitting on $column
			fi

			# check if the schema is dbo
			if [ "$schema" == "$SQLSERVER_DBO" ] || [ "$schema" == "$SQLSERVER_DBOC" ]; then
				echo Has dbo as schema
				TABLE=$table
			else
				echo Does NOT have dbo as schema
				TABLE=$schema.$table
			fi
			echo $TABLE
			# Determine if there was a column specified then call sqoop accordingly optionally passing in the --split-by value
			# Also checks if the number of mappers is specified
			if [ $column ]; then
				COLON_INDEX=$(expr index $column $COLON)
				echo Colon Index: $COLON_INDEX
				if [ $COLON_INDEX -gt 0 ]; then
					COLUMN_END_POS=$(($COLON_INDEX-1))
					START_MAPPER_POS=$(($COLON_INDEX+1))
					END_MAPPER_POS=$((${#column}-$COLON_INDEX))
					echo Full column length: ${#column}
					echo Start Mapper Pos: $START_MAPPER_POS
					echo End Mapper Pos: $END_MAPPER_POS
					COLUMN=$(expr substr $column 1 $COLUMN_END_POS)
					MAPPERS=$(expr substr $column $START_MAPPER_POS $END_MAPPER_POS)
				else
					COLUMN=$column
					MAPPERS=$DEFAULT_MAPPERS
				fi
				echo Column: $COLUMN
                               echo "uppper"
				echo Mappers: $MAPPERS
			else
                                COLON_INDEX=$(expr index $table $COLON)
                                echo Colon Index: $COLON_INDEX
                                if [ $COLON_INDEX -gt 0 ]; then
                                        COLUMN_END_POS=$(($COLON_INDEX-1))
                                        START_MAPPER_POS=$(($COLON_INDEX+1))
                                        END_MAPPER_POS=$((${#table}-$COLON_INDEX))
                                        echo Full column length: ${#column}
                                        echo Start Mapper Pos: $START_MAPPER_POS
                                        echo End Mapper Pos: $END_MAPPER_POS
                                        TABLE=$(expr substr $table 1 $COLUMN_END_POS)
                                        MAPPERS=$(expr substr $table $START_MAPPER_POS $END_MAPPER_POS)
					table=$TABLE
                                else
					MAPPERS=$DEFAULT_MAPPERS
                                fi
                                 echo $schema $SQLSERVER_DBO $SQLSERVER_DBOC
				if [ "$schema" != "$SQLSERVER_DBO" ] || [ "$schema" != "$SQLSERVER_DBOC" ]; then
				        echo "inner"
                                 	TABLE="$schema.$table"
				fi
                                echo Table: $TABLE
                                echo Mappers: $MAPPERS
                                echo "lower"   
			fi
                 if [ $hive_diff = 'N' ]; then   
                        # check if there is a query file for this table
                        queryFile=$QUERY_PATH/query-$SPLICE_SCHEMA-$table.sql
                        echo The query file is: $queryFile
                       if [ -f "$queryFile" ] || [ $sqooprequired = 'N' ]; then
                         query=$(cat $queryFile)
				
######### New Changes start ####################
				if [ $loadtype = 'I' ]; then #for incremental load
				   MAXFILE=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-max.sql
                                   echo "select max(end_run_date) from spliceadmin.last_run where schema_id = $schema_id and table_id = $table_id and status = 'SUCCESS';" > $MAXFILE
	                           LAST_RUN_DTM=`sqlshell.sh -f $MAXFILE | head -11 | tail -1 | awk '{print $1" "$2}'`
                                   rm $MAXFILE
				   echo $LAST_RUN_DTM
				   INCRCOLFILE1=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-INC1.sql
				   INCRCOLFILE2=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-INC2.txt
				   echo "select '#'||column_name||'#' from spliceadmin.refresh_control_tbl where schemaname = '$SPLICE_SCHEMA' and tablename = '$table';" > $INCRCOLFILE1
				   sqlshell.sh -f $INCRCOLFILE1 | grep '#' | grep -v 'splice> select' | awk -F# '{print $2}' > $INCRCOLFILE2
				   mapfile -t COLUMN_INCREMENTAL < $INCRCOLFILE2
				   echo ${#COLUMN_INCREMENTAL[@]}
				   rm $INCRCOLFILE1 $INCRCOLFILE2
				   where_condition=""
				   if [[ "${LAST_RUN_DTM}" = "NULL " ]]; then				   
				           if [ $sqooprequired = 'N' ] ; then
                                              importFile=$IMPORT_PATH/import-$SPLICE_SCHEMA-$table.sql
                                              cp $importFile $IMPORT_PATH/import-$SPLICE_SCHEMA-$table-temp.sql
                                              sed -i '/XXXX/d' $IMPORT_PATH/import-$SPLICE_SCHEMA-$table-temp.sql
                                              loadtype1=$loadtype
                                              loadtype=F
                                           else
                                                echo "NO LAST RUN - FULL LOAD"
                                                loadtype1=$loadtype
                                                loadtype=F
                                           fi

                                   else
				           if [ $sqooprequired = 'N' ]; then
                                                importFile=$IMPORT_PATH/import-$SPLICE_SCHEMA-$table.sql
                                               cp $importFile $IMPORT_PATH/import-$SPLICE_SCHEMA-$table-temp.sql
                                                sed -i "s/'XXXX'/'$LAST_RUN_DTM'/g" $IMPORT_PATH/import-$SPLICE_SCHEMA-$table-temp.sql
                                           else  	
                                               count=0
     					       for t in "${COLUMN_INCREMENTAL[@]}"
     					         do
        					  if [[ $count -eq 1 ]]; then
                					where_condition+=" and "
        					  fi

                                       		  if [ $sourcetype = 'O' ]; then 
       							where_condition+="$t>= TO_TIMESTAMP('$LAST_RUN_DTM', 'YYYY-MM-DD HH24:MI:SS.FF') and $t<= TO_TIMESTAMP('$JOB_RUN_DATE', 'YYYY-MM-DD HH24:MI:SS.FF')" #oracle
                                       		  else
                                        		where_condition+="$t>= '$LAST_RUN_DTM' and $t<= '$JOB_RUN_DATE'" #SQLSERVER
                                       		  fi
						     count=1
					        done
        			        	echo $where_condition
        			         	query=${query/'$CONDITIONS'/"$where_condition and \$CONDITIONS"}
                                          fi 
				   fi
				fi
######### New Changes End ####################		            
				echo $query
                        else
                                query=""
                                echo Query file not found. $query
                        fi

                        # Make sure the directories exist in hdfs, clear the directory, and set permissions
			freeout=`free`
			freeout1=`cat /proc/meminfo`
			freeout2=`ulimit -a`
			echo "$freeout"
			echo "$freeout1"
			echo "$freeout2"
                        sudo -su hdfs hadoop fs -mkdir -p /data/sqoop/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /data/sqoop/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -rm -skipTrash /data/sqoop/$SPLICE_SCHEMA/$table/*
                        sudo -su hdfs hadoop fs -mkdir -p /bad/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /bad/$SPLICE_SCHEMA/$table

			# exit script if an error is encountered
                        set -e

			echo The query param is: $query!
			echo The column param is: $column!
                      if [ $sqooprequired = 'Y' ]; then
			remark="Sqoop started"
			jobstartdate=$(date +"%Y-%m-%d %T.0")
			sqooprowcount=0
			splicerowcount=0
			sqlshell.sh << !
			insert into spliceadmin.last_run values('$SPLICE_SCHEMA','$splice_tablename','$jobstartdate',$sqooprowcount,$splicerowcount,'$frequency',$schema_id,$table_id,'RUNNING','$jobstartdate','$remark');
!
			if [ -z "$query" ]; then
                                if [ $column ]; then
                                        echo Execute sqoop with split-by
                                        sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/data/sqoop/keystore/hadoop-credentials-store --options-file $SQOOP_CONFIG_FILE --append --table $TABLE --split-by $COLUMN --target-dir /data/sqoop/$SPLICE_SCHEMA/$table --m $MAPPERS
                                else
                                        echo Execute sqoop without split-by
                                        sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/data/sqoop/keystore/hadoop-credentials-store --options-file $SQOOP_CONFIG_FILE --append --table $TABLE --target-dir /data/sqoop/$SPLICE_SCHEMA/$table --m $MAPPERS
                                fi
			else 
                                if [ $column ]; then
                                        echo Execute sqoop with split-by
                                        sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/data/sqoop/keystore/hadoop-credentials-store --options-file $SQOOP_CONFIG_FILE --append --query "$query" --split-by $COLUMN --target-dir /data/sqoop/$SPLICE_SCHEMA/$table --m $MAPPERS
                                else
                                        echo Sqoop extract for $SCHEMA.$TABLE failed because a query file is present but no column specified for the split-by.
					remark="Sqoop failed as split by column not specified"
					jobenddate=$(date +"%Y-%m-%d %T.0")
                        		sqlshell.sh << !
		                        update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                        exit 1
                                fi
			fi
		      	
			if [ $? -gt 0 ]; then
				echo Sqoop extract failed
                                remark="Sqoop failed"
				jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
				exit 1
			else
				echo Export of $SPLICE_SCHEMA.$table successful 
       		        	echo Importing $SPLICE_SCHEMA.$table to Splice Machine
                                remark="Sqoop Completed. Splice Import running."
				if [ $sqooprequired = 'Y' ] ; then
				   sqooprowcount=$(grep -i "mapreduce.ImportJobBase: Retrieved" $LOG_FILE | tail -1 | cut -d" " -f6)
				else
				   sqooprowcount=0
				fi
				jobenddate=$(date +"%Y-%m-%d %T.0")  
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'RUNNING', remark = '$remark', SQOOP_ROW_COUNT = $sqooprowcount, END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
			fi
		      fi
                     	# Import data from HDFS to Splice
		      if [ $sqooprequired = 'Y' ] ; then
                  	if [ $loadtype = 'I' ]; then ##Upsert
				IMPORT_FILE=$UPSERT_PATH/upsert-$SPLICE_SCHEMA-$table.sql
			else
				IMPORT_FILE=$IMPORT_PATH/import-$SPLICE_SCHEMA-$table.sql
			fi
                      else
                         IMPORT_FILE=$IMPORT_PATH/import-$SPLICE_SCHEMA-$table-temp.sql
                       fi

			if [ -f $IMPORT_FILE ]; then
                       		sqlshell.sh -f $IMPORT_FILE
				STATUS=$(grep "ERROR 42Y55\|ERROR XIE0M\|ERROR XJ001\|ERROR SE\|ERROR 08006\|ClosedChannelException\|ERROR 42Y03\|ERROR:\|ERROR 42Z23" $LOG_FILE | wc -l)
  echo $STATUS
				if [ $STATUS -gt 0 ]; then
					remark="Splice import failed"
					jobenddate=$(date +"%Y-%m-%d %T.0")
					sqlshell.sh << !
                                        update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                        echo Splice import failed
					exit 1
				else
                                sqlshell.sh << !
                                update $SPLICE_SCHEMA.$splice_tablename	set INSERT_LOAD_TS = '$jobenddate' where INSERT_LOAD_TS is null;
!
					echo Import of $SPLICE_SCHEMA.$table completed
					echo $SPLICE_SCHEMA.$table
				#	sed -i "/$schema.$loadtype1.$sourcetype.$sqooprequired.$table/d" $FILE_LIST-FAILED
					sed -i "/$schema_id.$table_id/d" $FILE_LIST-FAILED
#					grep -v "$schema.$table" $FILE_LIST-FAILED > $FILE_LIST-FAILED1
#					mv $FILE_LIST-FAILED1 $FILE_LIST-FAILED
					echo $SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-RC.sql
					SPLICETABLE=`grep -i 'SYSCS_UTIL' $IMPORT_FILE | cut -d ',' -f2 | cut -d\' -f2`
					TABLEROWCOUNT=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-RC.sql
					LASTRUNDETAIL=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-LR.sql
					echo "select '##'||trim(cast(count(*) as char(100)))||'##' from $SPLICE_SCHEMA.$SPLICETABLE;" > $TABLEROWCOUNT
					#row_count=`sqlshell.sh -f $TABLEROWCOUNT | grep '##' | grep -v '||trim'| awk -F'#' '{print $3}'`
					if [ ${#row_count} -eq 0 ];then
						echo "select '##'||trim(cast(count(*) as char(100)))||'##' from $SPLICE_SCHEMA.\"$SPLICETABLE\";" > $TABLEROWCOUNT
						#row_count=`sqlshell.sh -f $TABLEROWCOUNT | grep '##' | grep -v '||trim'| awk -F'#' '{print $3}'`
					fi
					echo "splice-row-count:" $row_count
					echo " update spliceadmin.last_run set status = 'SUCCESS', remark= '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';"
					remark="Splice import successfull"
					jobenddate=$(date +"%Y-%m-%d %T.0")
                                        sqlshell.sh << !
                                        update spliceadmin.last_run set status = 'SUCCESS', remark= '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
#					echo "insert into spliceadmin.last_run values('$SPLICE_SCHEMA','$SPLICETABLE','$JOB_RUN_DATE',$row_count,0);" > $LASTRUNDETAIL
#					sqlshell.sh -f $LASTRUNDETAIL
#					rm $LASTRUNDETAIL 
					rm $TABLEROWCOUNT
				fi
			else
				echo $IMPORT_FILE not found.... skipping import to Splice Machine
				remark="Import file not found."
				jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
				exit 1
			fi

			# do not exit script if there are errors
			set +e
		else 
             queryFile=$QUERY_PATH/query-$SPLICE_SCHEMA-$table.sql
                        echo The query file is: $queryFile
                          if [ -f "$queryFile" ]; then
                         query=$(cat $queryFile)
                       else
                                query=""
                                echo Query file not found. $query
                        fi

                        # Make sure the directories exist in hdfs, clear the directory, and set permissions
                        sudo -su hdfs hadoop fs -mkdir -p /data/hive/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /data/hive/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -rm -skipTrash /data/hive/$SPLICE_SCHEMA/$table/*
                        sudo -su hdfs hadoop fs -mkdir -p /bad/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /bad/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -mkdir -p /data/diff/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /data/diff/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -rm -skipTrash /data/diff/$SPLICE_SCHEMA/$table/*
                        remark="Sqoop started"
                        jobstartdate=$(date +"%Y-%m-%d %T.0")
                        sqooprowcount=0
                        splicerowcount=0
                        sqlshell.sh << !
                        insert into spliceadmin.last_run values('$SPLICE_SCHEMA','$splice_tablename','$jobstartdate',$sqooprowcount,$splicerowcount,'$frequency',$schema_id,$table_id,'RUNNING','$jobstartdate','$remark');
!
                        if [ -z "$query" ]; then
                                if [ $column ]; then
                                        echo Execute sqoop with split-by
                                        echo "first"
                                        sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/data/sqoop/keystore/hadoop-credentials-store --options-file $SQOOP_CONFIG_FILE --append --table $TABLE --split-by $COLUMN --target-dir /data/hive/$SPLICE_SCHEMA/$table --m $MAPPERS
                                else
                                        echo Execute sqoop without split-by
                                        sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/data/sqoop/keystore/hadoop-credentials-store --options-file $SQOOP_CONFIG_FILE --append --table $TABLE --target-dir /data/hive/$SPLICE_SCHEMA/$table --m $MAPPERS
                                fi
                        else
                                if [ $column ]; then
                                        echo Execute sqoop with split-by
                                        echo "second"
                                        sqoop import -Dhadoop.security.credential.provider.path=jceks://hdfs/data/sqoop/keystore/hadoop-credentials-store --options-file $SQOOP_CONFIG_FILE --append --query "$query" --split-by $COLUMN --target-dir /data/hive/$SPLICE_SCHEMA/$table --m $MAPPERS
                                else
                                        echo Sqoop extract for $SCHEMA.$TABLE failed because a query file is present but no column specified for the split-by.
                                        remark="Sqoop failed as split by column not specified"
                                        jobenddate=$(date +"%Y-%m-%d %T.0")
                                         sqlshell.sh << !
                                         update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                        exit 1
                                fi
                        fi
                         if [ $? -gt 0 ]; then
                                echo Sqoop extract failed
                                remark="Sqoop failed"
                                jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                exit 1
                        else
                                echo Export of $SPLICE_SCHEMA.$table successful
                                echo Importing $SPLICE_SCHEMA.$table to Splice Machine
                                remark="Sqoop Completed."
                                if [ $sqooprequired = 'Y' ] ; then
                                   sqooprowcount=$(grep -i "mapreduce.ImportJobBase: Retrieved" $LOG_FILE | tail -1 | cut -d" " -f6)
                                else
                                   sqooprowcount=0
                                fi
                                jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'RUNNING', remark = '$remark', SQOOP_ROW_COUNT = $sqooprowcount, END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                        fi
               rm /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt || true
         if [ $hive_cdc = 'N' ];then
                               # java -jar /home/splice/cetera/sqoop/diffkit/diffkit-hive-app.jar -planfiles //home/splice/cetera/sqoop/diffkit/conf/$SPLICE_SCHEMA/diffkitt-${SPLICE_SCHEMA}-${table}-hive.plan.xml 
                remark="Diffkit started."
                jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                ssh splice@papplsl040 "java -jar /data/diffkit-hive-app.jar -planfiles //data/conf/$SPLICE_SCHEMA/diffkitt-${SPLICE_SCHEMA}-${table}-hive.plan.xml"
                  if [ $? -gt 0 ]; then
                                echo diffkit failed
                                remark="Diffkit failed"
                                jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                exit 1
                 fi
         else
         
         sudo -su hdfs hive -f /home/splice/cetera/sqoop/$SPLICE_SCHEMA/hive-query/hive-query-$SPLICE_SCHEMA-$table.sql
                if [ $? -gt 0 ]; then
                    echo hive failed
                    remark="hive failed."
                    jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                     rm /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt || true          
                      exit 1
               fi
               
          
         fi
                            if [ $hive_cdc = 'N' ];then
                                #echo wait for few moments
                               # sleep 10
                               # sed -i '/^$/d' /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt
                                diff_count=$(wc -l /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt| cut -d ' ' -f1)
                                echo $diff_count
                                sudo -su hdfs hadoop fs -put /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt /data/diff/$SPLICE_SCHEMA/$table/
                            fi
                          if [ $loadtype = 'I' ]; then ##Upsert
                                IMPORT_FILE=$UPSERT_PATH/upsert-$SPLICE_SCHEMA-$table.sql
                        else
                                IMPORT_FILE=$IMPORT_PATH/import-$SPLICE_SCHEMA-$table.sql
                        fi
                             sqlshell.sh -f $IMPORT_FILE   
                          STATUS=$(grep "ERROR 42Y55\|ERROR XIE0M\|ERROR XJ001\|ERROR SE\|ERROR 08006\|ClosedChannelException\|ERROR 42Y03\|ERROR:\|ERROR 42Z23" $LOG_FILE | wc -l)
  echo $STATUS
   
                                if [ $STATUS -gt 0 ]; then
                                       echo Splice import failed
                                       rm /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt
                                       remark="Splice import failed"
                                       jobenddate=$(date +"%Y-%m-%d %T.0")
                                        sqlshell.sh << !
                                        update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                       exit 1
                                else
                                       echo Import of $SPLICE_SCHEMA.$table completed
                                        echo $SPLICE_SCHEMA.$table
                                #       sed -i "/$schema.$loadtype1.$sourcetype.$sqooprequired.$table/d" $FILE_LIST-FAILED
                                        sed -i "/$schema_id.$table_id/d" $FILE_LIST-FAILED
#                                       grep -v "$schema.$table" $FILE_LIST-FAILED > $FILE_LIST-FAILED1
#                                       mv $FILE_LIST-FAILED1 $FILE_LIST-FAILED
                                        echo $SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-RC.sql
                                        SPLICETABLE=`grep -i 'SYSCS_UTIL' $IMPORT_FILE | cut -d ',' -f2 | cut -d\' -f2`
                                        TABLEROWCOUNT=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-RC.sql
                                        LASTRUNDETAIL=/home/splice/cetera/sqoop/runtemp/$SPLICE_SCHEMA-$table-$JOB_RUN_DATE1-$loadtype1-LR.sql
                                        echo "select '##'||trim(cast(count(*) as char(100)))||'##' from $SPLICE_SCHEMA.$SPLICETABLE;" > $TABLEROWCOUNT
                                        #row_count=`sqlshell.sh -f $TABLEROWCOUNT | grep '##' | grep -v '||trim'| awk -F'#' '{print $3}'`
                                        if [ ${#row_count} -eq 0 ];then
                                                echo "select '##'||trim(cast(count(*) as char(100)))||'##' from $SPLICE_SCHEMA.\"$SPLICETABLE\";" > $TABLEROWCOUNT
                                         #       row_count=`sqlshell.sh -f $TABLEROWCOUNT | grep '##' | grep -v '||trim'| awk -F'#' '{print $3}'`
                                        fi
                                        echo "splice-row-count:" $row_count
                                        echo " update spliceadmin.last_run set  status = 'SUCCESS', remark= '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';"
                                        remark="Splice import successfull"
                                        jobenddate=$(date +"%Y-%m-%d %T.0")
                                        sqlshell.sh << !
                                        update spliceadmin.last_run set status = 'SUCCESS', remark= '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
#                                       echo "insert into spliceadmin.last_run values('$SPLICE_SCHEMA','$SPLICETABLE','$JOB_RUN_DATE',$row_count,0);" > $LASTRUNDETAIL
#                                       sqlshell.sh -f $LASTRUNDETAIL
#                                       rm $LASTRUNDETAIL
                                        rm $TABLEROWCOUNT 
                                fi
                    # hive -e "Truncate table ${SPLICE_SCHEMA}.${table}"
                   sudo -su hdfs hive -e "Truncate table ${SPLICE_SCHEMA}.${table}; INSERT INTO TABLE ${SPLICE_SCHEMA}.${table} select * from ${SPLICE_SCHEMA}.${table}_ext;"
                                      if [ $? -gt 0 ]; then
                    echo hive failed
                    remark="hive failed."
                    jobenddate=$(date +"%Y-%m-%d %T.0")
                                sqlshell.sh << !
                                update spliceadmin.last_run set status = 'FAILED', remark = '$remark', END_RUN_DATE = '$jobenddate' where schema_id = $schema_id and table_id = $table_id and START_RUN_DATE = '$jobstartdate';
!
                                       rm /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt || true
                                       exit 1
                                    fi

                        
                  rm /diffkit/diffdata-${SPLICE_SCHEMA}-${table}.txt || true
                   # sudo -su hdfs hadoop fs -rm -skipTrash /data/hive/$SPLICE_SCHEMA/$table/*
            fi	
                 				
		} < /dev/null; done
	fi
else
	echo Incorrect number of parameters specified to execute script
	echo "Usage: ./run-sqoop-full.sh <config file> <table list file> <splice schema name> <import file path> <log file name>"
	exit 1
fi

if [ $? -eq 0 ]; then
    echo Sqoop and Splice import job completed.
    rm $FILE_LIST-FAILED
    exit 0
else
  exit 1
fi
