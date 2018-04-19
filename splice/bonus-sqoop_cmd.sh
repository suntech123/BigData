#!/bin/bash
###########################################

###########################################
sqoop import --connect jdbc:oracle:thin:@//pdbolsl04-scan.one.ad:3004/svc_ADVBOP_etl --username bonusselect --password bonusselect_ctprod01 --table BONUS.TRADES --as-avrodatafile --map-column-java TRADE_DATE=String,CANCEL_DATE=String,RECEIVE_DATE=String,SETTLEMENT_DATE=String,POST_DATE=String,CREATE_DATE=String,LAST_MOD_DATE=String,PD_DATE=String -m 100 --target-dir /data/FLZ/import/sqoop/LREG/xyz21
