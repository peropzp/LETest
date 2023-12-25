with unn as (select data.machineId, datas.signalGroup, datas.type, val.timestamp, val.value,
PARSE_DATETIME('%s', CAST(TRUNC(val.timestamp) AS STRING)) as dt
from `test1.fendt_data` as data, unnest(datas) as datas, unnest(values) as val),
pivotedData as
(select unn.machineId, unn.timestamp,
AVG(IF(unn.type = 'TotalDEFConsumption', unn.value, null)) TotalDEFConsumption ,
AVG(IF(unn.type = 'WheelBasedVehicleSpeed', unn.value, null)) WheelBasedVehicleSpeed,
AVG(IF(unn.type = 'TransOilTemp', unn.value, null)) TransOilTemp,
AVG(IF(unn.type = 'OilPressure', unn.value, null)) OilPressure,
AVG(IF(unn.type = 'HYDR_OIL_LEVEL', unn.value, null)) HYDR_OIL_LEVEL,
AVG(IF(unn.type = 'LoadAtCurrSpeed', unn.value, null)) LoadAtCurrSpeed,
AVG(IF(unn.type = 'FuelLevel', unn.value, null)) FuelLevel,
AVG(IF(unn.type = 'BatteryPotentialSwitched', unn.value, null)) BatteryPotentialSwitched,
AVG(IF(unn.type = 'AirFilter', unn.value, null)) AirFilter,
AVG(IF(unn.type = 'EngineSpeed', unn.value, null)) EngineSpeed,
AVG(IF(unn.type = 'CoolantTemperature', unn.value, null)) CoolantTemperature,
AVG(IF(unn.type = 'CatalystTankLevel', unn.value, null)) CatalystTankLevel,
AVG(IF(unn.type = 'ENGINE_TRIP_FUEL', unn.value, null)) ENGINE_TRIP_FUEL,
AVG(IF(unn.type = 'FuelRate', unn.value, null)) FuelRate,
AVG(IF(unn.type = 'TOTAL_VEHICLE_HOURS', unn.value, null)) TOTAL_VEHICLE_HOURS,
AVG(IF(unn.type = 'HRSengineHours', unn.value, null)) HRSengineHours,
AVG(IF(unn.type = 'REAR_DRAFT', unn.value, null)) REAR_DRAFT,
AVG(IF(unn.type = 'OutdoorTemp', unn.value, null)) OutdoorTemp,
AVG(IF(unn.type = 'HITCH_POSITION_FRONT', unn.value, null)) HITCH_POSITION_FRONT,
AVG(IF(unn.type = 'HITCH_POSITION_REAR', unn.value, null)) HITCH_POSITION_REAR,
AVG(IF(unn.type = 'PLC_R_Measured_position', unn.value, null)) PLC_R_Measured_position,
AVG(IF(unn.type = 'PLC_R_Draft', unn.value, null)) PLC_R_Draft,
AVG(IF(unn.type = 'TotalFuelUsed', unn.value, null)) TotalFuelUsed,
AVG(IF(unn.type = 'GearOilFilter', unn.value, null)) GearOilFilter,
AVG(IF(unn.type = 'WHEEL_SLIP', unn.value, null)) WHEEL_SLIP,
AVG(IF(unn.type = 'WORK_ON', unn.value, null)) WORK_ON,
AVG(IF(unn.type = 'DiffLockState', unn.value, null)) DiffLockState
from unn
group by unn.machineId, unn.timestamp),
unnGps as
(SELECT gps.machineId, r.t, r.lat, r.lng
FROM `test1.fendt_gps`  as gps, unnest (route) as r),
joinedFendt as 
(select unnGps.t, unnGps.lat, unnGps.lng, pivotedData.*
from unnGps, pivotedData
where unnGps.machineId = pivotedData.MachineId and unnGps.t = pivotedData.timestamp)
select count(*) from joinedFendt

