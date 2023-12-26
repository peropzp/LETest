with  
  f as (select t as ft, machineId from test1.fendt order by machineId, ft),
  w as (select date_diff(datetime, timestamp('1970-01-01 00:00:00'), second) as dt, unit_id from `test1.wialon` order by unit_id, dt),
  mjoined as (
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and round(w.dt / 10) = round(f.ft / 10)),
fwjoin as 
    (select mjoined.machineId, mjoined.unit_id, mjoined.dt, max(mjoined.ft) as ft
    from mjoined 
    group by mjoined.machineId, mjoined.unit_id, mjoined.dt
    order by mjoined.machineId, mjoined.unit_id, mjoined.dt)

select *
from test1.fendt as f, test1.wialon as w, fwjoin
where f.machineId = fwjoin.machineID and f.t = fwjoin.ft 
and w.unit_id = fwjoin.unit_id and w.datetime = cast( PARSE_DATETIME('%s', CAST(TRUNC(fwjoin.dt) AS STRING)) as timestamp)
