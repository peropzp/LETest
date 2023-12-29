create or replace table LETestDataset1.MachineIdLink as
with 
linkFW as (
  select f.machineId, w.unit_id, count(*) as cnt
  from LETestDataset1.fendt as f, LETestDataset1.wialon as w
  where round(w.gpsLatitude * 1000000) = round(f.gpsLatitude * 1000000)
  and round(w.gpsLongitude * 1000000) = round(f.gpsLongitude * 1000000)
  and abs(datetime_diff(w.datetime, cast(PARSE_DATETIME('%s', CAST(TRUNC(f.timestamp) AS STRING)) as timestamp), SECOND)) < 10
  and w.speed = 0
  group by f.machineId, w.unit_id),
linkTW as 
  (select t.serialnumber, w.unit_id, count(*) as cnt
    from LETestDataset1.telematics as t, LETestDataset1.wialon as w
    where round(w.gpsLatitude * 100000) = round(t.gpsLatitude * 100000)
      and round(w.gpsLongitude * 100000) = round(t.gpsLongitude * 100000)
      and abs(datetime_diff(w.datetime, t.datetime, SECOND)) < 120
      and w.speed = 0
    group by t.serialnumber, w.unit_id)

select cast(l1.machineId as STRING) as machineId, l1.unit_id
from linkFW as l1
where l1.cnt = (select max(l2.cnt) from linkFW as l2 where l1.machineId = l2.machineId )

union all

select l1.serialnumber as machineId, l1.unit_id
from linkTW as l1
where l1.cnt = (select max(l2.cnt) from linkTW as l2 where l1.serialnumber = l2.serialnumber );

