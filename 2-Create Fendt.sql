create or replace table LETestDataset1.fendt as

with 
unnFendtData as (
  select data.machineId, datas.signalGroup, datas.type, val.timestamp, val.value,
    PARSE_DATETIME('%s', CAST(TRUNC(val.timestamp) AS STRING)) as dt
  from LETestDataset1.fendt_data as data, unnest(datas) as datas, unnest(values) as val),
pivotedData as (
  select unnFendtData.machineId, unnFendtData.Timestamp,
    AVG(IF(unnFendtData.type = 'TotalDEFConsumption', unnFendtData.value, null)) TotalDEFConsumption ,
    AVG(IF(unnFendtData.type = 'WheelBasedVehicleSpeed', unnFendtData.value, null)) SpeedGearbox_km_h,
    AVG(IF(unnFendtData.type = 'TransOilTemp', unnFendtData.value, null)) TransOilTemp,
    AVG(IF(unnFendtData.type = 'OilPressure', unnFendtData.value, null)) OilPressure,
    AVG(IF(unnFendtData.type = 'HYDR_OIL_LEVEL', unnFendtData.value, null)) HYDR_OIL_LEVEL,
    AVG(IF(unnFendtData.type = 'LoadAtCurrSpeed', unnFendtData.value, null)) EngineLoad,
    AVG(IF(unnFendtData.type = 'FuelLevel', unnFendtData.value, null)) FuelLevel,
    AVG(IF(unnFendtData.type = 'BatteryPotentialSwitched', unnFendtData.value, null)) BatteryPotentialSwitched,
    AVG(IF(unnFendtData.type = 'AirFilter', unnFendtData.value, null)) AirFilter,
    AVG(IF(unnFendtData.type = 'EngineSpeed', unnFendtData.value, null)) Engine_rpm,
    AVG(IF(unnFendtData.type = 'CoolantTemperature', unnFendtData.value, null)) TempCoolant_C,
    AVG(IF(unnFendtData.type = 'CatalystTankLevel', unnFendtData.value, null)) CatalystTankLevel,
    AVG(IF(unnFendtData.type = 'ENGINE_TRIP_FUEL', unnFendtData.value, null)) ENGINE_TRIP_FUEL,
    AVG(IF(unnFendtData.type = 'FuelRate', unnFendtData.value, null)) FuelConsumption_l_h,
    AVG(IF(unnFendtData.type = 'TOTAL_VEHICLE_HOURS', unnFendtData.value, null)) TotalWorkingHours,
    AVG(IF(unnFendtData.type = 'HRSengineHours', unnFendtData.value, null)) HRSengineHours,
    AVG(IF(unnFendtData.type = 'REAR_DRAFT', unnFendtData.value, null)) REAR_DRAFT,
    AVG(IF(unnFendtData.type = 'OutdoorTemp', unnFendtData.value, null)) OutdoorTemp,
    AVG(IF(unnFendtData.type = 'HITCH_POSITION_FRONT', unnFendtData.value, null)) HITCH_POSITION_FRONT,
    AVG(IF(unnFendtData.type = 'HITCH_POSITION_REAR', unnFendtData.value, null)) HITCH_POSITION_REAR,
    AVG(IF(unnFendtData.type = 'PLC_R_Measured_position', unnFendtData.value, null)) PLC_R_Measured_position,
    AVG(IF(unnFendtData.type = 'PLC_R_Draft', unnFendtData.value, null)) PLC_R_Draft,
    AVG(IF(unnFendtData.type = 'TotalFuelUsed', unnFendtData.value, null)) TotalFuelUsed,
    AVG(IF(unnFendtData.type = 'GearOilFilter', unnFendtData.value, null)) GearOilFilter,
    AVG(IF(unnFendtData.type = 'WHEEL_SLIP', unnFendtData.value, null)) WHEEL_SLIP,
    AVG(IF(unnFendtData.type = 'WORK_ON', unnFendtData.value, null)) WORK_ON,
    AVG(IF(unnFendtData.type = 'DiffLockState', unnFendtData.value, null)) DifferentialLockStatus
  from unnFendtData
  group by unnFendtData.machineId, unnFendtData.timestamp),
unnGps as (
  SELECT gps.machineId, r.t, r.lat, r.lng
  FROM LETestDataset1.fendt_gps  as gps, unnest (route) as r)

select unnGps.lat as GpsLatitude, unnGps.lng as GpsLongitude, pivotedData.*
  from unnGps, pivotedData
  where unnGps.machineId = pivotedData.MachineId and unnGps.t = pivotedData.timestamp
