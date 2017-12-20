add jar /home/kumarsu/hivexmlserde-1.0.5.3.jar;
insert overwrite directory '/data/MORNINGSTAR/CSV' row format delimited fields terminated by '\u0001' NULL DEFINED AS '' 
SELECT InvestmentVehicle_Id, CUSIP,ISIN,exp_HoldingDetail.HoldingDetail["Ticker"],exp_HoldingDetail.HoldingDetail["HoldingDetailId"],exp_HoldingDetail.HoldingDetail["DetailHoldingTypeId"],exp_HoldingDetail.HoldingDetail["SecurityName"],exp_HoldingDetail.HoldingDetail["Weighting"],exp_HoldingDetail.HoldingDetail["NumberOfShare"],exp_HoldingDetail.HoldingDetail["MarketValue"]
FROM MorningStar.MS_DataWarehouse_VA_HoldingDetail LATERAL VIEW explode(HoldingDetails) v1 AS exp_HoldingDetail;
