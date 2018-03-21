#!/bin/bash

RUNDATE=$(date +"%m-%d-%Y_%T")
TABLEDATE=$(date +"%m-%d-%Y")

if [[ ! ($SCHEMA && $FREQUENCY && $ACTIVE_FLAG) ]]; then

	if [ $# -eq 3 ]; then
		SCHEMA=$1
		FREQUENCY=$2
		ACTIVE_FLAG=$3
		RUNTYPE=1
	else
		RUNTYPE=0
	fi
else
	RUNTYPE=1
fi

if [[ $RUNTYPE -eq "1" ]]; then
	CONFIG=/home/splice/cetera/sqoop/$SCHEMA/$SCHEMA-config.txt
	TABLES=/home/splice/cetera/sqoop/runtemp/$SCHEMA-tables-$FREQUENCY-$RUNDATE.txt
        TABLE_CHECK=/home/splice/cetera/sqoop/runtemp/$SCHEMA-tables-$FREQUENCY-$TABLEDATE
	IMPORTDIR=/home/splice/cetera/sqoop/$SCHEMA/import
	UPSERTDIR=/home/splice/cetera/sqoop/$SCHEMA/upsert
	QUERYDIR=/home/splice/cetera/sqoop/$SCHEMA/query
	if [ $FREQUENCY = 'M' ]; then
          LOGFILE=/home/splice/cetera/sqoop/$SCHEMA/logs/extract-monthly-$SCHEMA-$RUNDATE.log
        elif [ $FREQUENCY = 'D' ]; then
          LOGFILE=/home/splice/cetera/sqoop/$SCHEMA/logs/extract-daily-$SCHEMA-$RUNDATE.log
        elif [ $FREQUENCY = 'W' ]; then
          LOGFILE=/home/splice/cetera/sqoop/$SCHEMA/logs/extract-weekly-$SCHEMA-$RUNDATE.log
        else
          LOGFILE=/home/splice/cetera/sqoop/$SCHEMA/logs/extract-adhoc-$SCHEMA-$RUNDATE.log
        fi

filecount=`ls -l $TABLE_CHECK* | wc -l`
if [[ $filecount > 2 ]]; then
	echo "Job already running with frequency $FREQUENCY . Exiting..."
 	exit 1
fi

if [ -f $TABLE_CHECK*-FAILED ]; then
	echo "Identified previous failed instance. Using table file from earlier run"
	mv $TABLE_CHECK*-FAILED $TABLES
        mv $TABLE_CHECK*-statistics $TABLES-statistics
else
	TABLES=/home/splice/cetera/sqoop/runtemp/$SCHEMA-tables-$FREQUENCY-$RUNDATE.txt
        SCHEMATEMP=/home/splice/cetera/sqoop/runtemp/$SCHEMA-$RUNDATE-$FREQUENCY-temp1.sql
        TABLETEMP=/home/splice/cetera/sqoop/runtemp/$SCHEMA-$RUNDATE-$FREQUENCY-temp2.sql
#	if [ $LOADTYPE ]; then
#		echo "Select trim(a.schemaname||'#'||a.table_owner||'.'||a.Loadtype||'.'||a.source_type||'.'||sqooprequired||'.'||a.tablename||case when a.splitcolumn is null then '' else '.' end||coalesce(a.splitcolumn,'')||case when a.mapper is null then '' else ':' end||coalesce(a.mapper,'')||'#')
#from spliceadmin.ingestion_control_tbl a where a.Frequency = '$FREQUENCY' and a.table_active_flag = '$ACTIVE_FLAG' and
#	a.schemaname = '$SCHEMA' and a.Loadtype = '$LOADTYPE';" > $SCHEMATEMP
#	else
	echo "Select trim(a.schemaname||'#'||trim(cast(cast(a.schema_id as char(10)) AS VARCHAR(10)))||'.'||trim(cast(cast(a.table_id as char(10)) AS VARCHAR(10)))||'.'||a.splice_tablename||'.'||a.frequency||'.'||a.table_owner||'.'||a.Loadtype||'.'||a.source_type||'.'||a.hive_diff||'.'||a.hive_cdc||'.'||a.sqooprequired||'.'||a.tablename||case when a.splitcolumn is null then '' else '.' end||coalesce(a.splitcolumn,'')||case when a.mapper is null then '' else ':' end||coalesce(a.mapper,'')||'#') from spliceadmin.ingestion_control_tbl a where a.Frequency = '$FREQUENCY' and a.table_active_flag = '$ACTIVE_FLAG' and a.schemaname = '$SCHEMA' order by a.order_by;" > $SCHEMATEMP
#	fi
	sqlshell.sh -f $SCHEMATEMP > $TABLETEMP
	grep '#' $TABLETEMP | grep -v 'splice>' | awk -F# '{print $2}' > $TABLES
        cut -d"." -f3 $TABLES > $TABLES-statistics
	rm $TABLETEMP $SCHEMATEMP
fi

/home/splice/cetera/sqoop/run-hive1-auto.sh $CONFIG $TABLES $SCHEMA $IMPORTDIR $UPSERTDIR $QUERYDIR $LOGFILE > $LOGFILE 2>&1
if [ $? -gt 0 ]; then
        echo run-sqoop-auto.sh failed
        rm $TABLES
        exit 1
else
        echo run-sqoop-auto.sh successful
	echo Starting statistics on $SCHEMA
	/home/splice/cetera/sqoop/run-table-compaction-statistics.sh $SCHEMA $TABLES-statistics >> $LOGFILE 2>&1
	echo Ingestion completed for $SCHEMA for frequency as $FREQUENCY.
	rm $TABLES $TABLES-statistics
	find /home/splice/cetera/sqoop/runtemp/*.txt-[F\s]* -maxdepth 1 -mtime +1 -type f -delete
	exit 0
fi
else
	echo "Please provide correct input params - SCHEMA FREQUENCY ACTIVE_FLAG"
	exit 1
fi
