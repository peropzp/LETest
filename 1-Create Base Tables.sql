CREATE or replace TABLE `LETestDataset1.telematics`
(
  DateTime TIMESTAMP,
  SerialNumber STRING,
  GpsLongitude FLOAT64,
  GpsLatitude FLOAT64,
  TotalWorkingHours FLOAT64,
  Engine_rpm FLOAT64,
  EngineLoad FLOAT64,
  FuelConsumption_l_h FLOAT64,
  SpeedGearbox_km_h FLOAT64,
  SpeedRadar_km_h FLOAT64,
  TempCoolant_C FLOAT64,
  PtoFront_rpm FLOAT64,
  PtoRear_rpm FLOAT64,
  GearShift FLOAT64,
  TempAmbient_C FLOAT64,
  ParkingBreakStatus STRING,
  DifferentialLockStatus STRING,
  AllWheelDriveStatus STRING,
  CreeperStatus STRING
);

CREATE or replace TABLE `LETestDataset1.wialon`
(
  unit_id INT64,
  dateTime TIMESTAMP,
  driverId INT64,
  gpsLongitude FLOAT64,
  gpsLatitude FLOAT64,
  speed INT64,
  altitude INT64,
  course INT64
);

CREATE or replace TABLE `LETestDataset1.fendt_gps`
(
  machineId INT64,
  route ARRAY<STRUCT<t INT64, lat FLOAT64, lng FLOAT64>>
);

CREATE or replace TABLE `LETestDataset1.fendt_data`
(
  machineId INT64,
  count INT64,
  datas ARRAY<STRUCT<unit STRING, signalGroup STRING, type STRING, values ARRAY<STRUCT<timestamp INT64, value FLOAT64>>, enumerations STRUCT<_4_0 STRING, _0_0 STRING, _1_0 STRING, _2_0 STRING, _3_0 STRING>, count INT64>>
);

CREATE or replace TABLE LETestDataset1.joinedData
(
  DateTime TIMESTAMP,
  MachineId STRING, #machineID
  GpsLongitude FLOAT64, 
  GpsLatitude FLOAT64,
  Speed INT64,
  Altitude INT64,
  Course INT64,
  TimeInSeconds INT64,
  ###########################3
  TotalWorkingHours FLOAT64,  #TOTAL_VEHICLE_HOURS
  Engine_rpm FLOAT64, #engineSpeed
  EngineLoad FLOAT64, #LoadAtCurrSpeed
  FuelConsumption_l_h FLOAT64, #fuelRate
  SpeedGearbox_km_h FLOAT64, #WheelBasedVehicleSpeed
  SpeedRadar_km_h FLOAT64, 
  TempCoolant_C FLOAT64,  #CoolantTemperature
  PtoFront_rpm FLOAT64,
  PtoRear_rpm FLOAT64,
  GearShift FLOAT64,
  TempAmbient_C FLOAT64,
  ParkingBreakStatus STRING,
  DifferentialLockStatus STRING,  #DiffLockState
  AllWheelDriveStatus STRING,
  CreeperStatus STRING,
  ####################
  TotalDEFConsumption FLOAT64 ,
  TransOilTemp FLOAT64,
  OilPressure FLOAT64,
  HYDR_OIL_LEVEL FLOAT64,
  FuelLevel FLOAT64,
  BatteryPotentialSwitched FLOAT64,
  AirFilter FLOAT64,
  CatalystTankLevel FLOAT64,
  ENGINE_TRIP_FUEL FLOAT64,
  HRSengineHours FLOAT64,
  REAR_DRAFT FLOAT64,
  OutdoorTemp FLOAT64,
  HITCH_POSITION_FRONT FLOAT64,
  HITCH_POSITION_REAR FLOAT64,
  PLC_R_Measured_position FLOAT64,
  PLC_R_Draft FLOAT64,
  TotalFuelUsed FLOAT64,
  GearOilFilter FLOAT64,
  WHEEL_SLIP FLOAT64,
  WORK_ON FLOAT64
);
