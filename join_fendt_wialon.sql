with  
  f as (select PARSE_DATETIME('%s', CAST(TRUNC(t) AS STRING)) as ft, 
    machineId from test1.fendt order by machineId, ft),
  w as (select cast(datetime as datetime) as dt, unit_id from `test1.wialon` order by unit_id, dt),
  mjoined as (
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 20 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 19 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 18 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 17 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 16 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 15 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 14 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 13 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 12 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 11 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 10 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 9 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 8 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 7 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 6 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 5 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 4 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 3 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 2 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_sub(f.ft, INTERVAL 1 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = f.ft
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 20 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 19 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 18 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 17 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 16 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 15 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 14 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 13 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 12 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 11 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 10 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 9 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 8 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 7 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 6 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 5 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 4 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 3 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 2 SECOND)
    union all
    select f.machineid, f.ft, w.dt, w.unit_id
    from f
    inner join test1.fendtWialon as fw on fw.machineId = f.machineId
    inner join w on w.unit_id = fw.unit_id and w.dt = datetime_add(f.ft, INTERVAL 1 SECOND)),
fwjoin as 
    (select mjoined.machineId, mjoined.unit_id, mjoined.dt, max(mjoined.ft) as ft
    from mjoined 
    group by mjoined.machineId, mjoined.unit_id, mjoined.dt
    order by mjoined.machineId, mjoined.unit_id, mjoined.dt)

select * 
from test1.fendt as f, test1.wialon as w, fwjoin
where f.machineId = fwjoin.machineID and PARSE_DATETIME('%s', CAST(TRUNC(f.t) AS STRING)) = fwjoin.ft 
and w.unit_id = fwjoin.unit_id and cast(w.datetime as datetime) = fwjoin.dt

