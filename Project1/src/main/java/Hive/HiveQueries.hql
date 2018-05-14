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

select productid, dateyear, avg(score) from(
	select productid, year(from_unixtime(time)) as dateyear,score from amazonproduct) as p2  
	where dateyear>=2003 and dateyear<=2012  group by productid;
	
	
select p1.productid, p2.productid, count(*)  
from (select productid,userid from amazonproduct) as p1 
join (select productid,userid from amazonproduct) as p2 
on p1.userid = p2.userid and p1.productid <> p2.productid group by p1.productid, p2.productid 
order by p2.productid;
	