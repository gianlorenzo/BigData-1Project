CREATE TABLE AMAZONPRODUCT (
	Id BIGINT, 
	ProductId STRING, 
	UserId STRING, 
	ProfileName STRING, 
	HelpfulnessNumerator INT, 
	HelpfulnessDenominator INT, 
	Score FLOAT, 
	TIME INT, 
	Summary STRING, 
	Text STRING ) row format delimited fields terminated by ',';
	

LOAD DATA INPATH '/user/hive/input/Reviews.csv' OVERWRITE INTO TABLE amazonproduct;
