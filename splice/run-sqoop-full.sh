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
QUERY_PATH=$5
LOG_FILE=$6

SQLSERVER_DBO="dbo"
COLON=":"
DEFAULT_MAPPERS="8"

# check for correct number of parameters
if [ $# -eq 6 ]; then
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
		# loop through th file list and parse each line	
		while IFS=. read schema table column
		do
			echo Exporting $schema.$table
			if [ $column ]; then
				echo Splitting on $column
			fi

			# check if the schema is dbo
			if [ "$schema" == "$SQLSERVER_DBO" ]; then
				echo Has dbo as schema
				TABLE=$table
			else
				echo Does NOT have dbo as schema
				TABLE=$schema.$table
			fi

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
				if [ "$schema" != "$SQLSERVER_DBO" ]; then
					TABLE=$schema.$table
				fi
                                echo Table: $TABLE
                                echo Mappers: $MAPPERS

			fi

                        # check if there is a query file for this table
                        queryFile=$QUERY_PATH/query-$SPLICE_SCHEMA-$table.sql
                        echo The query file is: $queryFile
                        if [ -f "$queryFile" ]; then
                                query=$(cat $queryFile)
                                echo $query
                        else
                                query=""
                                echo Query file not found. $query
                        fi

                        # Make sure the directories exist in hdfs, clear the directory, and set permissions
                        sudo -su hdfs hadoop fs -mkdir -p /data/sqoop/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /data/sqoop/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -rm -skipTrash /data/sqoop/$SPLICE_SCHEMA/$table/*
                        sudo -su hdfs hadoop fs -mkdir -p /bad/$SPLICE_SCHEMA/$table
                        sudo -su hdfs hadoop fs -chmod 777 /bad/$SPLICE_SCHEMA/$table

			# exit script if an error is encountered
                        set -e

			echo The query param is: $query!
			echo The column param is: $column!
 
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
                                        exit 1
                                fi
			fi
			
			if [ $? -gt 0 ]; then
				echo Sqoop extract failed
				exit 1
			else
				echo Export of $schema.$table successful 
       		        	echo Importing $SPLICE_SCHEMA.$table to Splice Machine
			fi
			# Import data from HDFS to Splice
			IMPORT_FILE=$IMPORT_PATH/import-$SPLICE_SCHEMA-$table.sql
			if [ -f $IMPORT_FILE ]; then
                      		sqlshell.sh -f $IMPORT_FILE
				STATUS=$(grep "ERROR SE\|ERROR 08006\|ClosedChannelException" $LOG_FILE | wc -l)
				if [ $STATUS -gt 0 ]; then
					echo Splice import failed
					exit 1
				else
					echo Import of $SPLICE_SCHEMA.$table completed
				fi
			else
				echo $IMPORT_FILE not found.... skipping import to Splice Machine
				exit 1
			fi

			# do not exit script if there are errors
			set +e
		done <$FILE_LIST
	fi
else
	echo Incorrect number of parameters specified to execute script
	echo "Usage: ./run-sqoop-full.sh <config file> <table list file> <splice schema name> <import file path> <log file name>"
	exit 1
fi

echo Sqoop job completed.
exit 0
