select productid, dateyear, avg(score) from(
	select productid, year(from_unixtime(time)) as dateyear,score from amazonproduct) as p2  
	where dateyear>=2003 and dateyear<=2012  group by productid, dateyear;
