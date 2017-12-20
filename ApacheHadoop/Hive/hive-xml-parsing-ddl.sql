create schema MorningStar;
add jar /home/kumarsu/hivexmlserde-1.0.5.3.jar;
CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_NonVA(
InvestmentVehicle_Id string,
InvestmentVehicle_Name string,
FundId string,
ShareClassId string,
LegalType string,
FundFamilyId string,
FundFamilyName string,
ShareClassType_Id string,
ShareClassType_Name string,
GlobalCategory_Name string,
GlobalCategory_Id string,
LegalName string,
InceptionDate string,
CUSIP string,
ISIN string,
CoveredCall string,
InverseFund string,
LeveragedFund string,
BrandingName string,
BrandingName_Id string,
PreviousFundName string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.InvestmentVehicle_Name"="/InvestmentVehicle/Operation/InvestmentVehicleName/text()"
,"column.xpath.FundId"="/InvestmentVehicle/Operation/FundId/text()"
,"column.xpath.ShareClassId"="/InvestmentVehicle/Operation/ShareClassId/text()"
,"column.xpath.LegalType"="/InvestmentVehicle/Operation/LegalType/text()"
,"column.xpath.FundFamilyId"="/InvestmentVehicle/Operation/FundFamilyName/@_Id"
,"column.xpath.FundFamilyName"="/InvestmentVehicle/Operation/FundFamilyName/text()"
,"column.xpath.ShareClassType_Id"="/InvestmentVehicle/Operation/ShareClassId/text()"
,"column.xpath.ShareClassType_Name"="/InvestmentVehicle/Operation/ShareClassType/text()"
,"column.xpath.GlobalCategory_Name"="/InvestmentVehicle/Operation/GlobalCategoryName/text()"
,"column.xpath.GlobalCategory_Id"="/InvestmentVehicle/Operation/GlobalCategoryId/text()"
,"column.xpath.LegalName"="/InvestmentVehicle/Operation/LegalName/text()"
,"column.xpath.InceptionDate"="/InvestmentVehicle/Operation/InceptionDate/text()"
,"column.xpath.CUSIP"="/InvestmentVehicle/Operation/CUSIP/text()"
,"column.xpath.ISIN"="/InvestmentVehicle/Operation/ISIN/text()"
,"column.xpath.CoveredCall"="/InvestmentVehicle/Operation/CoveredCall/text()"
,"column.xpath.InverseFund"="/InvestmentVehicle/Operation/InverseFund/text()"
,"column.xpath.LeveragedFund"="/InvestmentVehicle/Operation/LeveragedFund/text()"
,"column.xpath.BrandingName"="/InvestmentVehicle/Operation/BrandingName/text()"
,"column.xpath.BrandingName_Id"="/InvestmentVehicle/Operation/BrandingName/@Id"
,"column.xpath.PreviousFundName"="/InvestmentVehicle/Operation/PreviousName/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/NonVA'
TBLPROPERTIES (
"serialization.null.format"='',
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_Datawarehouse_NonVA_Category(
InvestmentVehicle_Id  string,
MSCategory_Name  string,
MSCategory_Id  string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.MSCategory_Name"="/InvestmentVehicle/Operation/MorningstarCategory/Name/text()"
,"column.xpath.MSCategory_Id"="/InvestmentVehicle/Operation/MorningstarCategory/Id/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/NonVA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_NonVA_HoldingDetail(
InvestmentVehicle_Id string,
CUSIP string,
ISIN string,
HoldingDetails array<struct<HoldingDetail:map<string,string>>>
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.CUSIP"="/InvestmentVehicle/Operation/CUSIP/text()"
,"column.xpath.ISIN"="/InvestmentVehicle/Operation/ISIN/text()"
,"column.xpath.HoldingDetails"="/InvestmentVehicle/Portfolio/HoldingBreakdown/Holding/HoldingDetail"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/NonVA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_NonVA_RatingAndRiskMeasurements(
InvestmentVehicle_Id string,
RiskRatingOverall string,
RiskScoreOverall string,
Volatility string,
ArithmeticMeanY1 string,
StandardDeviationY1 string,
SkewnessY1 string,
KurtosisY1 string,
SharpeRatioY1 string,
SortinoRatioY1 string,
TrackingErrorY1 string,
InformationRatioY1 string,
CaptureRatioUpY1 string,
CaptureRatioDownY1 string,
BattingAverageY1 string,
ArithmeticMeanY3 string,
StandardDeviationY3 string,
SkewnessY3 string,
KurtosisY3 string,
SharpeRatioY3 string,
SortinoRatioY3 string,
TrackingErrorY3 string,
InformationRatioY3 string,
CaptureRatioUpY3 string,
CaptureRatioDownY3 string,
BattingAverageY3 string,
ArithmeticMeanY5 string,
StandardDeviationY5 string,
SkewnessY5 string,
KurtosisY5 string,
SharpeRatioY5 string,
SortinoRatioY5 string,
TrackingErrorY5 string,
InformationRatioY5 string,
CaptureRatioUpY5 string,
CaptureRatioDownY5 string,
BattingAverageY5 string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.RiskRatingOverall"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/RiskRatingOverall/text()"
,"column.xpath.RiskScoreOverall"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/RiskScoreOverall/text()"
,"column.xpath.Volatility"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/Volatility/text()"
,"column.xpath.ArithmeticMeanY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/ArithmeticMeanY1/text()"
,"column.xpath.StandardDeviationY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/StandardDeviationY1/text()"
,"column.xpath.SkewnessY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SkewnessY1/text()"
,"column.xpath.KurtosisY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/KurtosisY1/text()"
,"column.xpath.SharpeRatioY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SharpeRatioY1/text()"
,"column.xpath.SortinoRatioY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SortinoRatioY1/text()"
,"column.xpath.TrackingErrorY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/TrackingErrorY1/text()"
,"column.xpath.InformationRatioY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/InformationRatioY1/text()"
,"column.xpath.CaptureRatioUpY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioUpY1/text()"
,"column.xpath.CaptureRatioDownY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioDownY1/text()"
,"column.xpath.BattingAverageY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/BattingAverageY1/text()"
,"column.xpath.ArithmeticMeanY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/ArithmeticMeanY3/text()"
,"column.xpath.StandardDeviationY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/StandardDeviationY3/text()"
,"column.xpath.SkewnessY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SkewnessY3/text()"
,"column.xpath.KurtosisY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/KurtosisY3/text()"
,"column.xpath.SharpeRatioY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SharpeRatioY3/text()"
,"column.xpath.SortinoRatioY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SortinoRatioY3/text()"
,"column.xpath.TrackingErrorY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/TrackingErrorY3/text()"
,"column.xpath.InformationRatioY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/InformationRatioY3/text()"
,"column.xpath.CaptureRatioUpY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioUpY3/text()"
,"column.xpath.CaptureRatioDownY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioDownY3/text()"
,"column.xpath.BattingAverageY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/BattingAverageY3/text()"
,"column.xpath.ArithmeticMeanY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/ArithmeticMeanY5/text()"
,"column.xpath.StandardDeviationY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/StandardDeviationY5/text()"
,"column.xpath.SkewnessY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SkewnessY5/text()"
,"column.xpath.KurtosisY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/KurtosisY5/text()"
,"column.xpath.SharpeRatioY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SharpeRatioY5/text()"
,"column.xpath.SortinoRatioY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SortinoRatioY5/text()"
,"column.xpath.TrackingErrorY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/TrackingErrorY5/text()"
,"column.xpath.InformationRatioY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/InformationRatioY5/text()"
,"column.xpath.CaptureRatioUpY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioUpY5/text()"
,"column.xpath.CaptureRatioDownY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioDownY5/text()"
,"column.xpath.BattingAverageY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/BattingAverageY5/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/NonVA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_NonVA_TrailingPerformance(
InvestmentVehicle_Id  string,
PerformanceAsOfDate  string,
PerformanceId  string,
ClosePrice  string,
TrailingReturnYTD  string,
TrailingReturnY1  string,
TrailingReturnY2  string,
TrailingReturnY3  string,
TrailingReturnY4  string,
TrailingReturnY5  string,
TrailingReturnSinceInception  string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.PerformanceAsOfDate"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/PerformanceAsOfDate/text()"
,"column.xpath.PerformanceId"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/PerformanceId/text()"
,"column.xpath.ClosePrice"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/ClosePrice/text()"
,"column.xpath.TrailingReturnYTD"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnYTD/text()"
,"column.xpath.TrailingReturnY1"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY1/text()"
,"column.xpath.TrailingReturnY2"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY2/text()"
,"column.xpath.TrailingReturnY3"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY3/text()"
,"column.xpath.TrailingReturnY4"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY4/text()"
,"column.xpath.TrailingReturnY5"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY5/text()"
,"column.xpath.TrailingReturnSinceInception"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnSinceInception/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/NonVA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);
CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_VA(
InvestmentVehicle_Id string,
InvestmentVehicle_Name string,
FundId string,
ShareClassId string,
LegalType string,
FundFamilyId string,
FundFamilyName string,
ShareClassType_Id string,
ShareClassType_Name string,
GlobalCategory_Name string,
GlobalCategory_Id string,
LegalName string,
InceptionDate string,
CUSIP string,
ISIN string,
CoveredCall string,
InverseFund string,
LeveragedFund string,
BrandingName string,
BrandingName_Id string,
PreviousFundName string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.InvestmentVehicle_Name"="/InvestmentVehicle/Operation/InvestmentVehicleName/text()"
,"column.xpath.FundId"="/InvestmentVehicle/Operation/FundId/text()"
,"column.xpath.ShareClassId"="/InvestmentVehicle/Operation/ShareClassId/text()"
,"column.xpath.LegalType"="/InvestmentVehicle/Operation/LegalType/text()"
,"column.xpath.FundFamilyId"="/InvestmentVehicle/Operation/FundFamilyName/@_Id"
,"column.xpath.FundFamilyName"="/InvestmentVehicle/Operation/FundFamilyName/text()"
,"column.xpath.ShareClassType_Id"="/InvestmentVehicle/Operation/ShareClassId/text()"
,"column.xpath.ShareClassType_Name"="/InvestmentVehicle/Operation/ShareClassType/text()"
,"column.xpath.GlobalCategory_Name"="/InvestmentVehicle/Operation/GlobalCategoryName/text()"
,"column.xpath.GlobalCategory_Id"="/InvestmentVehicle/Operation/GlobalCategoryId/text()"
,"column.xpath.LegalName"="/InvestmentVehicle/Operation/LegalName/text()"
,"column.xpath.InceptionDate"="/InvestmentVehicle/Operation/InceptionDate/text()"
,"column.xpath.CUSIP"="/InvestmentVehicle/Operation/CUSIP/text()"
,"column.xpath.ISIN"="/InvestmentVehicle/Operation/ISIN/text()"
,"column.xpath.CoveredCall"="/InvestmentVehicle/Operation/CoveredCall/text()"
,"column.xpath.InverseFund"="/InvestmentVehicle/Operation/InverseFund/text()"
,"column.xpath.LeveragedFund"="/InvestmentVehicle/Operation/LeveragedFund/text()"
,"column.xpath.BrandingName"="/InvestmentVehicle/Operation/BrandingName/text()"
,"column.xpath.BrandingName_Id"="/InvestmentVehicle/Operation/BrandingName/@Id"
,"column.xpath.PreviousFundName"="/InvestmentVehicle/Operation/PreviousName/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/VA'
TBLPROPERTIES (
"serialization.null.format"='',
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_Datawarehouse_VA_Category(
InvestmentVehicle_Id  string,
MSCategory_Name  string,
MSCategory_Id  string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.MSCategory_Name"="/InvestmentVehicle/Operation/MorningstarCategory/Name/text()"
,"column.xpath.MSCategory_Id"="/InvestmentVehicle/Operation/MorningstarCategory/Id/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/VA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_VA_HoldingDetail(
InvestmentVehicle_Id string,
CUSIP string,
ISIN string,
HoldingDetails array<struct<HoldingDetail:map<string,string>>>
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.CUSIP"="/InvestmentVehicle/Operation/CUSIP/text()"
,"column.xpath.ISIN"="/InvestmentVehicle/Operation/ISIN/text()"
,"column.xpath.HoldingDetails"="/InvestmentVehicle/Portfolio/HoldingBreakdown/Holding/HoldingDetail"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/VA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_VA_RatingAndRiskMeasurements(
InvestmentVehicle_Id string,
RiskRatingOverall string,
RiskScoreOverall string,
Volatility string,
ArithmeticMeanY1 string,
StandardDeviationY1 string,
SkewnessY1 string,
KurtosisY1 string,
SharpeRatioY1 string,
SortinoRatioY1 string,
TrackingErrorY1 string,
InformationRatioY1 string,
CaptureRatioUpY1 string,
CaptureRatioDownY1 string,
BattingAverageY1 string,
ArithmeticMeanY3 string,
StandardDeviationY3 string,
SkewnessY3 string,
KurtosisY3 string,
SharpeRatioY3 string,
SortinoRatioY3 string,
TrackingErrorY3 string,
InformationRatioY3 string,
CaptureRatioUpY3 string,
CaptureRatioDownY3 string,
BattingAverageY3 string,
ArithmeticMeanY5 string,
StandardDeviationY5 string,
SkewnessY5 string,
KurtosisY5 string,
SharpeRatioY5 string,
SortinoRatioY5 string,
TrackingErrorY5 string,
InformationRatioY5 string,
CaptureRatioUpY5 string,
CaptureRatioDownY5 string,
BattingAverageY5 string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.RiskRatingOverall"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/RiskRatingOverall/text()"
,"column.xpath.RiskScoreOverall"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/RiskScoreOverall/text()"
,"column.xpath.Volatility"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/Volatility/text()"
,"column.xpath.ArithmeticMeanY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/ArithmeticMeanY1/text()"
,"column.xpath.StandardDeviationY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/StandardDeviationY1/text()"
,"column.xpath.SkewnessY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SkewnessY1/text()"
,"column.xpath.KurtosisY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/KurtosisY1/text()"
,"column.xpath.SharpeRatioY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SharpeRatioY1/text()"
,"column.xpath.SortinoRatioY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SortinoRatioY1/text()"
,"column.xpath.TrackingErrorY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/TrackingErrorY1/text()"
,"column.xpath.InformationRatioY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/InformationRatioY1/text()"
,"column.xpath.CaptureRatioUpY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioUpY1/text()"
,"column.xpath.CaptureRatioDownY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioDownY1/text()"
,"column.xpath.BattingAverageY1"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/BattingAverageY1/text()"
,"column.xpath.ArithmeticMeanY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/ArithmeticMeanY3/text()"
,"column.xpath.StandardDeviationY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/StandardDeviationY3/text()"
,"column.xpath.SkewnessY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SkewnessY3/text()"
,"column.xpath.KurtosisY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/KurtosisY3/text()"
,"column.xpath.SharpeRatioY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SharpeRatioY3/text()"
,"column.xpath.SortinoRatioY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SortinoRatioY3/text()"
,"column.xpath.TrackingErrorY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/TrackingErrorY3/text()"
,"column.xpath.InformationRatioY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/InformationRatioY3/text()"
,"column.xpath.CaptureRatioUpY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioUpY3/text()"
,"column.xpath.CaptureRatioDownY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioDownY3/text()"
,"column.xpath.BattingAverageY3"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/BattingAverageY3/text()"
,"column.xpath.ArithmeticMeanY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/ArithmeticMeanY5/text()"
,"column.xpath.StandardDeviationY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/StandardDeviationY5/text()"
,"column.xpath.SkewnessY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SkewnessY5/text()"
,"column.xpath.KurtosisY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/KurtosisY5/text()"
,"column.xpath.SharpeRatioY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SharpeRatioY5/text()"
,"column.xpath.SortinoRatioY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/SortinoRatioY5/text()"
,"column.xpath.TrackingErrorY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/TrackingErrorY5/text()"
,"column.xpath.InformationRatioY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/InformationRatioY5/text()"
,"column.xpath.CaptureRatioUpY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioUpY5/text()"
,"column.xpath.CaptureRatioDownY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/CaptureRatioDownY5/text()"
,"column.xpath.BattingAverageY5"="/InvestmentVehicle/TrailingPerformance/RatingAndRiskMeasurements/BattingAverageY5/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/VA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);

CREATE EXTERNAL TABLE MorningStar.MS_DataWarehouse_VA_TrailingPerformance(
InvestmentVehicle_Id  string,
PerformanceAsOfDate  string,
PerformanceId  string,
ClosePrice  string,
TrailingReturnYTD  string,
TrailingReturnY1  string,
TrailingReturnY2  string,
TrailingReturnY3  string,
TrailingReturnY4  string,
TrailingReturnY5  string,
TrailingReturnSinceInception  string
)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES(
"column.xpath.InvestmentVehicle_Id"="/InvestmentVehicle/@_Id"
,"column.xpath.PerformanceAsOfDate"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/PerformanceAsOfDate/text()"
,"column.xpath.PerformanceId"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/PerformanceId/text()"
,"column.xpath.ClosePrice"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/ClosePrice/text()"
,"column.xpath.TrailingReturnYTD"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnYTD/text()"
,"column.xpath.TrailingReturnY1"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY1/text()"
,"column.xpath.TrailingReturnY2"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY2/text()"
,"column.xpath.TrailingReturnY3"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY3/text()"
,"column.xpath.TrailingReturnY4"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY4/text()"
,"column.xpath.TrailingReturnY5"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnY5/text()"
,"column.xpath.TrailingReturnSinceInception"="/InvestmentVehicle/TrailingPerformance/MonthEndTrailingPerformance/TrailingReturnSinceInception/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION '/data/MORNINGSTAR/VA'
TBLPROPERTIES (
"xmlinput.start"="<InvestmentVehicle",
"xmlinput.end"="</InvestmentVehicle>"
);
