set @@dataset_project_id = 'letest-409019';
set @@dataset_id = 'LETestDataset';

delete from joinedData where 1=1;

insert into joinedData
with  
  f as (select timestamp as ft, machineId from fendt order by machineId, ft),
  t as (select date_diff(datetime, timestamp('1970-01-01 00:00:00'), second) as tt, SerialNumber as machineId from telematics order by machineId, tt),
  w as (select date_diff(datetime, timestamp('1970-01-01 00:00:00'), second) as wt, unit_id from wialon order by unit_id, wt),
  mfwjoined as (
    select f.machineid, f.ft, w.wt, w.unit_id
    from f
    inner join MachineIdLink as fw on fw.machineId = cast(f.machineId as STRING)
    inner join w on w.unit_id = fw.unit_id and round(w.wt / 10) = round(f.ft / 10)),
  fwjoin as (
    select mfwjoined.machineId, mfwjoined.unit_id, mfwjoined.wt, max(mfwjoined.ft) as ft
    from mfwjoined 
    group by mfwjoined.machineId, mfwjoined.unit_id, mfwjoined.wt
    order by mfwjoined.machineId, mfwjoined.unit_id, mfwjoined.wt),
  mtwjoined as (
    select t.machineid, t.tt, w.wt, w.unit_id
    from t
    inner join MachineIdLink as fw on fw.machineId = cast(t.machineId as STRING)
    inner join w on w.unit_id = fw.unit_id and round(w.wt / 10) = round(t.tt / 10)),
  twjoin as (
    select mtwjoined.machineId, mtwjoined.unit_id, mtwjoined.wt, max(mtwjoined.tt) as tt
    from mtwjoined 
    group by mtwjoined.machineId, mtwjoined.unit_id, mtwjoined.wt
    order by mtwjoined.machineId, mtwjoined.unit_id, mtwjoined.wt)

select
  w.datetime,
  cast(f.MachineId as STRING), #machineID
  w.GpsLongitude , 
  w.GpsLatitude ,
  w.Speed ,
  w.Altitude ,
  w.Course ,
  ##########################
  f.TotalWorkingHours ,  #TOTAL_VEHICLE_HOURS
  f.Engine_rpm , #engineSpeed
  f.EngineLoad , #LoadAtCurrSpeed
  f.FuelConsumption_l_h , #fuelRate
  f.SpeedGearbox_km_h , #WheelBasedVehicleSpeed
  null, 
  f.TempCoolant_C ,  #CoolantTemperature
  null,
  null,
  null,
  null,
  null,
  cast(f.DifferentialLockStatus as string) ,  #DiffLockState
  null,
  null,
  ####################
  f.TotalDEFConsumption  ,
  f.TransOilTemp ,
  f.OilPressure ,
  f.HYDR_OIL_LEVEL ,
  f.FuelLevel ,
  f.BatteryPotentialSwitched ,
  f.AirFilter ,
  f.CatalystTankLevel ,
  f.ENGINE_TRIP_FUEL ,
  f.HRSengineHours ,
  f.REAR_DRAFT ,
  f.OutdoorTemp ,
  f.HITCH_POSITION_FRONT ,
  f.HITCH_POSITION_REAR ,
  f.PLC_R_Measured_position ,
  f.PLC_R_Draft ,
  f.TotalFuelUsed ,
  f.GearOilFilter ,
  f.WHEEL_SLIP ,
  f.WORK_ON 
from fendt as f, wialon as w, fwjoin
where f.machineId = fwjoin.machineID and f.timestamp = fwjoin.ft 
and w.unit_id = fwjoin.unit_id and w.datetime = cast( PARSE_DATETIME('%s', CAST(TRUNC(fwjoin.wt) AS STRING)) as timestamp)

union all

select
  w.datetime,
  t.SerialNumber, #machineID
  w.GpsLongitude , 
  w.GpsLatitude ,
  w.Speed ,
  w.Altitude ,
  w.Course,
  ##########################3 
  TotalWorkingHours ,  #TOTAL_VEHICLE_HOURS
  Engine_rpm , #engineSpeed
  EngineLoad , #LoadAtCurrSpeed
  FuelConsumption_l_h , #fuelRate
  SpeedGearbox_km_h , #WheelBasedVehicleSpeed
  SpeedRadar_km_h , 
  TempCoolant_C ,  #CoolantTemperature
  PtoFront_rpm ,
  PtoRear_rpm ,
  GearShift ,
  TempAmbient_C ,
  ParkingBreakStatus ,
  DifferentialLockStatus ,  #DiffLockState
  AllWheelDriveStatus ,
  CreeperStatus ,
  ####################
  null,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null ,
  null 
from telematics as t, wialon as w, twjoin
where t.SerialNumber = twjoin.machineID and date_diff(t.datetime, timestamp('1970-01-01 00:00:00'), SECOND) = twjoin.tt 
and w.unit_id = twjoin.unit_id and w.datetime = cast( PARSE_DATETIME('%s', CAST(TRUNC(twjoin.wt) AS STRING)) as timestamp);
