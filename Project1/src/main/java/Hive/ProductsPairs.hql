select p1.productid, p2.productid, count(*)  
from (select productid,userid from amazonproduct) as p1 
join (select productid,userid from amazonproduct) as p2 
on p1.userid = p2.userid and p1.productid <> p2.productid group by p1.productid, p2.productid 
order by p1.productid;

