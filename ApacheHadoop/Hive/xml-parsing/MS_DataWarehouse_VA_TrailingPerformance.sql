add jar /home/kumarsu/hivexmlserde-1.0.5.3.jar;
insert overwrite directory '/data/MORNINGSTAR/CSV' row format delimited fields terminated by '\u0001' NULL DEFINED AS ''
select * from MorningStar.MS_DataWarehouse_VA_TrailingPerformance;
