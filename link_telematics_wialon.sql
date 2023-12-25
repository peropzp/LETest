create or replace table test1.telematicsWialon as 

with link as 
  (select t.serialnumber, w.unit_id, count(*) as cnt
    from `test1.telematics` as t, `test1.wialon` as w
    where round(w.gpsLatitude * 100000) = round(t.gpsLatitude * 100000)
      and round(w.gpsLongitude * 100000) = round(t.gpsLongitude * 100000)
      and abs(datetime_diff(w.datetime, t.datetime, SECOND)) < 120
      and w.speed = 0
    group by t.serialnumber, w.unit_id)

select l1.serialnumber, l1.unit_id
from link as l1
where l1.cnt = (select max(l2.cnt) from link as l2 where l1.serialnumber = l2.serialnumber )
order by l1.serialnumber, cnt desc;
