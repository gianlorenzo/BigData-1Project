select dateyear, wordstext, frequency from ( 
select allfrequency.*, row_number() over (partition by dateyear order by frequency desc) as numberinrow 
from (select dateyear, wordstext, count(*) as frequency from(select year(from_unixtime(time)) 
as dateyear, wordstext from amazonproduct lateral view explode(split(summary, ' ')) amazonproduct as wordstext)
 as p1 group by dateyear, wordstext) as allfrequency) as p2 where numberinrow<11;
