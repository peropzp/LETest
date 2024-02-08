set @@dataset_project_id = 'letest-409019';
set @@dataset_id = 'LETestDataset';


create or replace table turns as
with 
rndCourse as (  #round course
  select machineId, datetime, round(course / 20) * 20 as course
  from cleanedGsData
  order by MachineId, datetime
),
prevNextCourse as ( #prev&next point course
  select machineId, datetime, course,
  LEAD(course) OVER (PARTITION BY MachineId ORDER BY datetime) AS nextCourse,
  LAG(course) OVER (PARTITION BY MachineId ORDER BY datetime) AS prevCourse
  from rndCourse
),
addStartStop as ( #add start and stop when couse changes 
  select machineId, datetime,
  case 
      when (course <> prevCourse and course = nextCourse) then 'start'
      when (course = prevCourse and course <> nextCourse) then 'stop'
      else ''
  end as turn
  from prevNextCourse
  order by machineId, datetime
),
startStop as ( #select only start/stop points
  select * from addStartStop
  where turn = 'start' or turn = 'stop'
),
intervals as ( #get start/stop in one line
  select machineId, datetime as startTurnTime, turn as startTurnState, 
        Lead(datetime) over (PARTITION BY MachineId ORDER BY datetime) AS stopTurnTime,
        Lead(turn) over (PARTITION BY MachineId ORDER BY datetime) AS stopTurnState,
  from startstop
)

select machineId, startTurnTime, stopTurnTime #get only intervals which start with start, stop with stop and are last a leatst 60s
  from intervals 
  where startTurnState = 'start' and stopTurnState = 'stop'
  and date_diff(stopTurnTime, startTurnTime, SECOND) > 60
  order by machineId, startTurnTime
;

###############################################################

create or replace table TurnsGps as #get gps data for start/stop intervals
  select gd.MachineId, t.startTurnTime, t.stopTurnTime,
          gd.datetime, gd.gpsLongitude, gd.gpsLatitude, gd.course
  
  from turns as t, cleanedGsData as gd
  
  where t.MachineId = gd.MachineId 
    and gd.datetime > t.startTurnTime and gd.datetime < t.stopTurnTime;

#############################################################

create or replace table turnStats as #get point count, avg course, midpoint for intervals
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

create or replace table WorkTimes as #get intervals with at least 20 points
with courseRounded as (
  select machineId, startTurnTime, stopTurnTime, midPoint, round(avgCourse/20)*20 as rndCourse 
  from turnStats
  where cnt > 20
),
prevNextTurns as ( #get prev and next turn interval in one line
  select machineId, startTurnTime, stopTurnTime, midPoint, rndCourse,
    Lead(rndCourse) over (PARTITION BY MachineId ORDER BY startTurnTime) AS nextCourse,
    Lead(midPoint) over (PARTITION BY MachineId ORDER BY startTurnTime) AS nextMidPoint,
    Lead(startTurnTime) over (PARTITION BY MachineId ORDER BY startTurnTime) AS nextStartTurnTime,
    Lead(stopTurnTime) over (PARTITION BY MachineId ORDER BY startTurnTime) AS nextStopTurnTime,
    Lag(rndCourse) over (PARTITION BY MachineId ORDER BY startTurnTime) AS prevCourse,
    Lag(midPoint) over (PARTITION BY MachineId ORDER BY startTurnTime) AS prevMidPoint,
    Lag(startTurnTime) over (PARTITION BY MachineId ORDER BY startTurnTime) AS prevStartTurnTime,
    Lag(stopTurnTime) over (PARTITION BY MachineId ORDER BY startTurnTime) AS prevStopTurnTime,
    from courseRounded
),
addWorkStartStop as ( #add start and stop markers for turns according to oposite rndCourse 
  select machineId, startTurnTime, stopTurnTime,
      case 
      when (abs(rndCourse - prevCourse) <> 180 and abs(rndCourse - nextCourse) = 180) then 'start'
      when (abs(rndCourse - prevCourse) = 180 and abs(rndCourse - nextCourse) <> 180) then 'stop'
 #     when abs(date_diff(prevStopTurnTime, startTurnTime, SECOND)) > 300 then 'start'
 #     when abs(date_diff(nextStartTurnTime, stopTurnTime, SECOND)) > 300 then 'stop'
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
),
filterStarStopOrder as (
  select * from joinStartStops
  where startWorking = 'start' and stopWorking = 'stop'
),
joinWorkTimesWithTurnStats as (
  select wt.machineId, wt.startWorkingTime, wt.stopWorkingTime, count(*) cnt
  from filterStarStopOrder as wt, turnStats as ts
  where wt.machineId = ts.machineId and wt.startWorkingTime <= ts.startTurnTime and wt.stopWorkingTime >= ts.stopTurnTime
  group by wt.machineId, wt.startWorkingTime, wt.stopWorkingTime
)
select machineId, startWorkingTime, stopWorkingTime
from joinWorkTimesWithTurnStats
where cnt > 3
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
