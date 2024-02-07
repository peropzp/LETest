set @@dataset_project_id = 'letest-409019';
set @@dataset_id = 'LETestDataset';


create or replace table turns as
with 
rndCourse as (
  select machineId, datetime, round(course / 20) * 20 as course
  from cleanedGsData
  order by MachineId, datetime
),
prevNextCourse as (
  select machineId, datetime, course,
  LEAD(course) OVER (PARTITION BY MachineId ORDER BY datetime) AS nextCourse,
  LAG(course) OVER (PARTITION BY MachineId ORDER BY datetime) AS prevCourse
  from rndCourse
),
addStartStop as (
  select machineId, datetime,
  case 
      when (course <> prevCourse and course = nextCourse) then 'start'
      when (course = prevCourse and course <> nextCourse) then 'stop'
      else ''
  end as turn
  from prevNextCourse
  order by machineId, datetime
),
startStop as (
  select * from addStartStop
  where turn = 'start' or turn = 'stop'
),
intervals as (
  select machineId, datetime as startTurnTime, turn as startTurnState, 
        Lead(datetime) over (PARTITION BY MachineId ORDER BY datetime) AS stopTurnTime,
        Lead(turn) over (PARTITION BY MachineId ORDER BY datetime) AS stopTurnState,
  from startstop
)

select machineId, startTurnTime, stopTurnTime
  from intervals 
  where startTurnState = 'start' and stopTurnState = 'stop'
  and date_diff(stopTurnTime, startTurnTime, SECOND) > 60
  order by machineId, startTurnTime
;

###############################################################

create or replace table TurnsGps as
  select gd.MachineId, t.startTurnTime, t.stopTurnTime,
          gd.datetime, gd.gpsLongitude, gd.gpsLatitude, gd.course
  
  from turns as t, cleanedGsData as gd
  
  where t.MachineId = gd.MachineId 
    and gd.datetime > t.startTurnTime and gd.datetime < t.stopTurnTime;

#############################################################

create or replace table turnStats as
select machineId, startTurnTime, stopTurnTime,
      count(*) as cnt, avg(course) as avgCourse,
      ST_GEOGFROMTEXT(
        concat('POINT(',avg(gpsLongitude), ' ', avg(gpsLongitude), ')')
      ) as midpoint
      ####################### mogao bih da dodam tacku pocetka i mozda kraja zbbog selekcije sledeceg turna
      from TurnsGps
      group by machineId, startTurnTime, stopTurnTime
      order by machineId, startTurnTime, stopTurnTime
;

###############################################################################333333

create or replace table WorkTimes as
with courseRounded as (
  select machineId, startTurnTime, stopTurnTime, midPoint, round(avgCourse/20)*20 as rndCourse 
  from turnStats
  where cnt > 20
),
prevNextTurns as (
  select machineId, startTurnTime, stopTurnTime, midPoint, rndCourse,
    Lead(rndCourse) over (PARTITION BY MachineId ORDER BY startTurnTime) AS nextCourse,
    Lead(midPoint) over (PARTITION BY MachineId ORDER BY startTurnTime) AS nextMidPoint,
    Lag(rndCourse) over (PARTITION BY MachineId ORDER BY startTurnTime) AS prevCourse,
    Lag(midPoint) over (PARTITION BY MachineId ORDER BY startTurnTime) AS prevMidPoint
    from courseRounded
),
addWorkStartStop as (
  select machineId, startTurnTime, stopTurnTime,
      case 
      when (abs(rndCourse - prevCourse) <> 180 and abs(rndCourse - nextCourse) = 180) then 'start'
      when (abs(rndCourse - prevCourse) = 180 and abs(rndCourse - nextCourse) <> 180) then 'stop'
      else ''
  end as working
  from prevNextTurns
),
filterWorkStartStop as (
  select * from addWorkStartStop
  where working = 'start' or working = 'stop'
),
joinStartStops as (
  select machineId, startTurnTime as startWorkingTime, working as startWorking,
    lead(stopTurnTime) over (PARTITION BY machineId order by startTurnTime) as stopWorkingTime, 
    lead(working) over (PARTITION BY machineId order by startTurnTime) as stopWorking
  from filterWorkStartStop
)
select * from joinStartStops
where startWorking = 'start' and stopWorking = 'stop'
;

#####################################################################

create or replace table WorksWithGps as
  select w.MachineId, w.startWorkingTime, w.stopWorkingTime,  
      cgd.gpsLongitude, cgd.gpsLatitude, cgd.datetime
  from WorkTimes as w, cleanedGsData as cgd
  where w.MachineId = cgd.MachineId and cgd.datetime > w.startWorkingTime and cgd.datetime < w.stopWorkingTime
  order by w.MachineId, w.startWorkingTime
; 

#######################################################################

create or replace table multilines as
with
joinedStrings as (
  select MachineId, startWorkingTime, stopWorkingTime, 
    concat('LINESTRING(',string_agg(concat(gpsLongitude, ' ', gpsLatitude), ',' order by datetime), ')') as multiline,
    concat('POLYGON((',string_agg(concat(gpsLongitude, ' ', gpsLatitude), ',' order by datetime), '))') as polygon
  from WorksWithGps 
  group by MachineId, startWorkingTime, stopWorkingTime
)
select MachineId, startWorkingTime, stopWorkingTime,
    st_geogfromtext(multiline, make_valid => TRUE) as multiline,
    st_geogfromtext(polygon, make_valid => TRUE) as polygon
from joinedStrings
