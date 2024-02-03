set @@dataset_project_id = 'letest-409019';
set @@dataset_id = 'LETestDataset';


create or replace table trips as
with
prevnext as (
  select machineId, DateTime, gpsLongitude, gpsLatitude,
  LEAD(gpsLongitude) OVER (PARTITION BY MachineId ORDER BY datetime) AS nextGpsLongitude,
  LAG(gpsLongitude) OVER (PARTITION BY MachineId ORDER BY datetime) AS prevGpsLongitude,
  LEAD(gpsLatitude) OVER (PARTITION BY MachineId ORDER BY datetime) AS nextGpsLatitude,
  LAG(gpsLatitude) OVER (PARTITION BY MachineId ORDER BY datetime) AS prevGpsLatitude,
  LEAD(datetime) OVER (PARTITION BY MachineId ORDER BY datetime) AS nextDatetime,
  LAG(datetime) OVER (PARTITION BY MachineId ORDER BY datetime) AS prevDatetime
  from cleanedGsData
  order by MachineId, datetime
),
addStartStop as (
  select machineId, datetime, gpsLongitude, gpsLatitude,
  case 
      when (abs(gpsLongitude * 500 - prevGpsLongitude * 500) >1) then 'start'
      when (abs(gpsLatitude * 500 - prevGpsLatitude * 500)>1) then 'start'
      when (abs(gpsLongitude * 500 - nextGpsLongitude * 500)>1) then 'stop'
      when (abs(gpsLatitude * 500 - nextGpsLatitude * 500)>1) then 'stop'
      when (abs(date_diff(datetime, prevDatetime, second))>600) then 'start'
      when (abs(date_diff(datetime, nextDatetime, second))>600) then 'stop'
      else ''
  end as running
  from prevnext
  order by machineId, datetime
),
startStop as (
  select * from addStartStop
  where running = 'start' or running = 'stop'
),
intervals as (
  select MachineId, datetime as startDateTime, running as startState, gpsLongitude as startLongitude, gpsLatitude as startLatitude,
        Lead(datetime) over (PARTITION BY MachineId ORDER BY datetime) AS stopDateTime,
        Lead(running) over (PARTITION BY MachineId ORDER BY datetime) AS stopState,
        Lead(gpsLongitude) over (PARTITION BY MachineId ORDER BY datetime) AS stopLongitude,
        Lead(gpsLatitude) over (PARTITION BY MachineId ORDER BY datetime) AS stopLatitude
  from startstop
),
fixedIntervals as (
  select * from intervals where startState = 'start' and stopState = 'stop'
)
select * from fixedIntervals;

###############################################################

create or replace table tripsWithGps as
  select i.MachineId, i.startDateTime, i.StopDateTime, i.startLongitude, i.startLatitude, i.stopLongitude, i.stopLatitude, j.datetime, 
      j.gpsLongitude, j.gpsLatitude, j.course
  from trips as i, cleanedGsData as j
  where i.MachineId = j.MachineId and j.datetime > i.startDateTime and j.datetime < i.stopDateTime
  order by MachineId, i.startDateTime, j.datetime; 

###############################################################

create or replace table turns as
with 
rndCourse as (
  select machineId, startDateTime, datetime, round(course / 20) * 20 as course
  from tripsWithGps
  order by MachineId, datetime
),
prevNextCourse as (
  select machineId, startDateTime, datetime, course,
  LEAD(course) OVER (PARTITION BY MachineId, startDateTime ORDER BY datetime) AS nextCourse,
  LAG(course) OVER (PARTITION BY MachineId, startDateTime ORDER BY datetime) AS prevCourse
  from rndCourse
),
addStartStop as (
  select machineId, startDateTime, datetime,
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
  select machineId, startDateTime, datetime as startTurnTime, turn as startTurnState, 
        Lead(datetime) over (PARTITION BY MachineId ORDER BY datetime) AS stopTurnTime,
        Lead(turn) over (PARTITION BY MachineId ORDER BY datetime) AS stopTurnState,
  from startstop
)

select machineId, startDateTime, startTurnTime, stopTurnTime
  from intervals 
  where startTurnState = 'start' and stopTurnState = 'stop'
  order by machineId, startDateTime, startTurnTime
;

###############################################################

create or replace table TripsTurnsGps as
  select tg.MachineId, tg.startDateTime, tg.StopDateTime, tg.startLongitude, tg.startLatitude, tg.stopLongitude, tg.stopLatitude, 
          t.turnStartTime, t.turnStopTime,
          tg.datetime, tg.gpsLongitude, tg.gpsLatitude, tg.course
  
  from turns as t, tripsWithGps as tg
  
  where t.MachineId = tg.MachineId and t.startDatetime = tg.startDatetime
    and tg.datetime > t.turnStartTime and tg.datetime < turnStopTime;

#############################################################

create or replace table turnStats as
select machineId, startDateTime, turnStartTime,
      count(*) as cnt, avg(course) as course, avg(gpsLongitude) as avgLongitude, avg(gpsLatitude) as avgLatitude,
      ST_GEOGFROMTEXT(
        concat('POINT(',avg(gpsLongitude), ' ', avg(gpsLongitude), ')')
      ) as midPoint
      from TripsTurnsGps
      group by machineId, startDateTime, turnStartTime
      order by machineId, startDateTime, turnStartTime
;

###############################################################################333333

create or replace table WorkTimes as
with courseRounded as (
  select machineId, startDateTime, turnStartTime, cnt, avgLongitude, avgLatitude, midPoint, round(course/20)*20 as rndCourse 
  from turnStats
  where cnt > 20
),
prevNextTurns as (
  select machineId, startDateTime, turnStartTime, cnt, avgLongitude, avgLatitude, midPoint, rndCourse,
    Lead(rndCourse) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS nextCourse,
    Lead(avgLongitude) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS nextLongitude,
    Lead(avgLatitude) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS nextLatitude,
    Lead(midPoint) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS nextMidPoint,
    Lag(rndCourse) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS prevCourse,
    Lag(avgLongitude) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS prevLongitude,
    Lag(avgLatitude) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS prevLatitude,
    Lag(midPoint) over (PARTITION BY MachineId, startDateTime ORDER BY turnStartTime) AS prevMidPoint
    from courseRounded
),
filteredTurns as (
  select machineId, startdateTime, turnStartTime
  from prevNextTurns
  where prevCourse = nextCourse and abs(rndCourse - nextCourse) = 180
)
select * from filteredTurns
order by machineId, startdateTime, turnStartTime
#select machineId, startDateTime, min(turnStartTime) as workStart, max(turnStartTime) as workEnd
#  from filteredTurns
#  group by machineId, startDateTime
;

#####################################################################

create or replace table multilines as
with
joinedStrings as (
  select ttg.MachineId, ttg.startDateTime,
    concat('LINESTRING(',string_agg(concat(ttg.gpsLongitude, ' ', ttg.gpsLatitude), ',' order by datetime), ')') as multiline,
    concat('POLYGON((',string_agg(concat(ttg.gpsLongitude, ' ', ttg.gpsLatitude), ',' order by datetime), '))') as polygon,
    count(*) as cnt
  from TripsTurnsGps as ttg, WorkTimes wt
  where wt.machineId = ttg.machineId and wt.startDateTime = ttg.startDateTime and wt.turnStartTime = ttg.turnStartTime
  group by MachineId, startDateTime
)
select MachineId, startDateTime, 
    st_geogfromtext(multiline, make_valid => TRUE) as multiline,
    st_geogfromtext(polygon, make_valid => TRUE) as polygon
from joinedStrings
where cnt>10
