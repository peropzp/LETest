create or replace table test1.fendtWialon as
with link as 
  (select f.machineId, w.unit_id, count(*) as cnt
    from `test1.fendt` as f, `test1.wialon` as w
    where round(w.gpsLatitude * 1000000) = round(f.lat * 1000000)
    and round(w.gpsLongitude * 1000000) = round(f.lng * 1000000)
    and abs(datetime_diff(w.datetime, cast(PARSE_DATETIME('%s', CAST(TRUNC(f.timestamp) AS STRING)) as timestamp), SECOND)) < 10
    and w.speed = 0
  group by f.machineId, w.unit_id)
select l1.machineId, l1.unit_id
from link as l1
where l1.cnt = (select max(l2.cnt) from link as l2 where l1.machineId = l2.machineId )
order by l1.machineId, cnt desc;
