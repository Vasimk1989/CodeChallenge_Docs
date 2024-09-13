
  --=========================
-- Azure Synapse - Stage Zone
  --=========================

-- Creating stage Schema

CREATE SCHEMA stg;

-- Device Raw data into Stage table

CREATE TABLE stg.Device
(
	DeviceId UNIQUEIDENTIFIER NOT NULL,
	Name NVARCHAR(250) NULL,
	CreatedAt DATETIME NOT NULL
)

-- Device Reading Raw data into Stage table

CREATE TABLE stg.DeviceReading
(
	DeviceId UNIQUEIDENTIFIER NOT NULL,
	CurrentValue NUMERIC(18,4) NULL,
	UNIT NVARCHAR(50) NULL,
	ReadingTimestamp DATETIME NULL,
	Version NUMERIC(18,0) NULL
)

  --==================================
-- Azure Synapse - Data Warehouse Zone
  --==================================

CREATE SCHEMA dw;

--Device Dimension Table

CREATE TABLE dw.dim_Device
(
	DeviceKey INT IDENTITY(1,1),
	DeviceId UNIQUEIDENTIFIER NOT NULL,
	DeviceName NVARCHAR(250) NULL,
	DeviceCreatedDatetime DATETIME NULL,
	IsActiveYN TINYINT NOT NULL,
	CreatedInstant DATETIME NOT NULL,
	LastUpdateInstant DATETIME NOT NULL
)  

  --DeviceRading Fact Table

CREATE TABLE dw.fact_DeviceReading
(
	DeviceKey INT NOT NULL,
	CurrentValue NUMERIC(18,4) NULL,
	UNIT NVARCHAR(50) NULL,
	ReadingGeneratedDatetime DATETIME NULL,
	Version NUMERIC(18,0) NULL,
	CreatedInstant DATETIME NOT NULL,
	LastUpdateInstant DATETIME NOT NULL
)



  --================
  -- QUERY PLAN (DWH)
  --================

  --INCREMENTAL LOAD INTO DIMENSION DEVICE TABLE

  INSERT INTO dw.dim_Device
  (
	  DeviceId,
	  DeviceName,
	  DeviceCreatedDate,
	  IsActiveYN,
	  IsDeletedYN,
	  CreatedInstant
  )
  SELECT 
	  D.DeviceId,
	  D.Name,
	  D.CreatedAt,
	  1 AS IsActiveYN,
	  GETDATE() AS CreatedInstant
  FROM stg.Device D LEFT JOIN dw.dim_Device DD ON D.DeviceId=DD.DeviceId
  WHERE DD.DeviceId IS NULL;

  
  --INSERTING INFERRED ROWS INTO DIMENSION TABLE

  ;WITH UNIQUE_DEVICEID_CTE AS
  (
	SELECT DISTINCT DeviceId FROM stg.DeviceReading
  )
  INSERT INTO dw.dim_Device
  (
	  DeviceId,
	  DeviceName,
	  DeviceCreatedDate,
	  IsActiveYN,
	  IsDeletedYN,
	  CreatedInstant
  )
  SELECT 
	  DC.DeviceId,
	  NULL AS DeviceName,
	  NULL AS CreatedAt,
	  1 AS IsActiveYN,
	  GETDATE() AS CreatedInstant
  FROM UNIQUE_DEVICEID_CTE DC 
  LEFT JOIN dw.dim_Device DD ON DC.DeviceId=DD.DeviceId
  WHERE DD.DeviceId IS NULL;


  --UPDATE THE DIMENSION DEVICE TABLE

  UPDATE DD
  SET DD.DeviceName=D.Name, 
	  DD.DeviceCreatedDate=D.CreatedAt,
	  DD.LastUpdateInstant=GETDATE()
  FROM stg.Device D INNER JOIN dw.dim_Device DD ON D.DeviceId=DD.DeviceId
  WHERE DD.DeviceName<>D.Name OR DD.DeviceCreatedDate<>D.CreatedAt;


  --INCREMENTAL LOAD INTO FACT DEVICE READING TABLE

  WITH STG_DEVICEREADING_CTE AS
  (
	SELECT 
		DD.DeviceKey,
		DR.DeviceId,
		DR.CurrentValue,
		DR.UNIT,
		DR.ReadingTimestamp,
		DR.Version
	FROM stg.DeviceReading DR
	INNER JOIN dw.dim_Device DD ON DR.DeviceId=DD.DeviceId
  )
  INSERT INTO dw.fact_DeviceReading
  (
		DeviceKey,
		CurrentValue,
		UNIT,
		ReadingGeneratedDatetime,
		Version,
		CreatedInstant,
		LastUpdateInstant
  )
  SELECT 
	SDR.DeviceKey,
	SDR.CurrentValue,
	SDR.UNIT,
	SDR.ReadingTimestamp,
	SDR.Version,
	GETDATE() AS CreatedInstant
  FROM STG_DEVICEREADING_CTE SDR 
  LEFT JOIN dw.fact_DeviceReading FDR ON SDR.DeviceKey = FDR.DeviceKey AND SDR.ReadingTimestamp=FDR.ReadingGeneratedDatetime
  WHERE FDR.DeviceKey IS NULL;


  --UPDATE THE FACT DEVICE READING TABLE

  WITH STG_DEVICEREADING_CTE AS
  (
	SELECT 
		DD.DeviceKey,
		DR.DeviceId,
		DR.CurrentValue,
		DR.UNIT,
		DR.ReadingTimestamp,
		DR.Version
	FROM stg.DeviceReading DR
	INNER JOIN dw.dim_Device DD ON DR.DeviceId=DD.DeviceId
  )
  UPDATE FDR
  SET FDR.CurrentValue=SDR.CurrentValue, 
	  FDR.UNIT=SDR.UNIT,
	  FDR.Version=SDR.Version,
	  FDR.LastUpdateInstant=GETDATE()
  FROM dw.fact_DeviceReading FDR 
  INNER JOIN STG_DEVICEREADING_CTE SDR ON SDR.DeviceKey = FDR.DeviceKey AND SDR.ReadingTimestamp=FDR.ReadingGeneratedDatetime
  WHERE SDR.CurrentValue<>FDR.CurrentValue OR SDR.UNIT<>FDR.UNIT OR SDR.Version<>FDR.Version;


  -- Creating Index

CREATE INDEX IX_DeviceKey ON dw.fact_DeviceReading (DeviceKey);

CREATE INDEX IX_d_Device_Key ON dw.dim_Device(DeviceKey);


--======================
-- Data Retrieval Query
--======================
SELECT 
    DD..DeviceName, 
    FDR.CurrentValue, 
    FDR.ReadingTimestamp,
    FDR.Unit
FROM 
   dw.fact_DeviceReading FDR
INNER JOIN 
   dw.dim_Device DD ON FDR.DeviceKey = DD.DeviceKey ;

