-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### (A) Create Live Bronze Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### (A.1) Create Incremental Live Bronze Table
-- MAGIC
-- MAGIC <p>Data will be incrementally loaded from Data Lake using Auto Loader<\p>

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE YellowTaxis_BronzeLiveIncremental (
  RideId INT COMMENT "This is the primary key column",
  VendorId INT,
  PickupTime TIMESTAMP,
  DropTime TIMESTAMP,
  PickupLocationId INT,
  DropLocationId INT,
  CabNumber STRING,
  DriverLicenseNumber STRING,
  PassengerCount INT,
  TripDistance DOUBLE,
  RatecodeId INT,
  PaymentType INT,
  TotalAmount DOUBLE,
  FareAmount DOUBLE,
  Extra DOUBLE,
  MtaTax DOUBLE,
  TipAmount DOUBLE,
  TollsAmount DOUBLE,
  ImprovementSurcharge DOUBLE,
  FileName STRING,
  CreatedOn TIMESTAMP
)
USING DELTA
LOCATION "dbfs:/mnt/datalake/Output/YellowTaxis_BronzeLiveIncremental.delta"
PARTITIONED BY (VendorId)
COMMENT "Live Bronze table for YellowTaxis"
AS
SELECT
  RideId :: Int,
  VendorId :: Int,
  PickupTime :: Timestamp,
  DropTime :: Timestamp,
  PickupLocationId :: Int,
  DropLocationId :: Int,
  CabNumber :: String,
  DriverLicenseNumber :: String,
  PassengerCount :: Int,
  TripDistance :: Double,
  RateCodeId :: Int,
  PaymentType :: Int,
  TotalAmount :: Double,
  FareAmount :: Double,
  Extra :: Double,
  MtaTax :: Double,
  TipAmount :: Double,
  TollsAmount :: Double,
  ImprovementSurcharge :: Double,
  INPUT_FILE_NAME() AS FileName,
  CURRENT_TIMESTAMP() AS CreatedOn
FROM
  cloud_files(
    "dbfs:/mnt/raw/deep_dive/YellowTaxis/",
    "csv",
    map("inferSchema", "true")
  );

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### (A.2) Create Incremental Live Bronze View
-- MAGIC
-- MAGIC <p>This is to apply data quality checks and fetch incremental data from Bronze table<\p>
-- MAGIC
-- MAGIC <p>To pickup data from Bronze table incrementally you have to use STREAM() function and pass it the table name<\p>

-- COMMAND ----------

CREATE INCREMENTAL LIVE VIEW YellowTaxis_BronzeLiveIncrementalView (
  CONSTRAINT Valid_TotalAmount EXPECT (
    TotalAmount IS NOT NULL
    AND TotalAmount > 0
  ) ON VIOLATION DROP ROW,
  CONSTRAINT Valid_TripDistance EXPECT (TripDistance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT Valid_RideId EXPECT (
    RideId IS NOT NULL
    AND RideId > 0
  ) ON VIOLATION FAIL
  UPDATE
) AS
SELECT
  RideId,
  VendorId,
  PickupTime,
  DropTime,
  PickupLocationId,
  DropLocationId,
  TripDistance,
  TotalAmount,
  CreatedOn,
  YEAR(PickupTime) AS PickupYear,
  MONTH(PickupTime) AS PickupMonth,
  Day(PickupTime) AS PickupDay
FROM
  STREAM(live.YellowTaxis_BronzeLiveIncremental);

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (B) Create Live Silver Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### (B.1) Create Incremental Live Silver Table
-- MAGIC
-- MAGIC <p>If you are building and incremental table, there are 2 options:<\p>
-- MAGIC <p>1. If you only want to append the new data, you should define the SELECT statement<\p>
-- MAGIC <p>2. If you want to merge the data, do not define SELECT statement<\p>

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE YellowTaxis_SilverLiveIncremental (
  RideId INT COMMENT "This is the primary key column",
  VendorId INT,
  PickupTime TIMESTAMP,
  DropTime TIMESTAMP,
  PickupLocationId INT,
  DropLocationId INT,
  TripDistance DOUBLE,
  TotalAmount DOUBLE,
  CreatedOn TIMESTAMP,
  PickupYear INT,
  PickupMonth INT,
  PickupDay INT
)
USING DELTA
LOCATION "dbfs:/mnt/datalake/Output/YellowTaxis_SilverLiveIncremental.delta"
PARTITIONED BY (PickupLocationId);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### (B.2) Merge change incrementally  into Silver Table
-- MAGIC
-- MAGIC <p>In DLT, there are no MERGE statement. You have to use APPLY CHANGES command<\p>

-- COMMAND ----------

APPLY CHANGES INTO live.YellowTaxis_SilverLiveIncremental
FROM STREAM(live.YellowTaxis_BronzeLiveIncrementalView)
KEYS(RideId, VendorId)
SEQUENCE BY CreatedOn;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (C) Create Live Gold Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### (C.1) Create Complete Gold Table

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_SummaryByDate_GoldLive2
LOCATION "dbfs:/mnt/datalake/Output/YellowTaxis_SummaryByDate_GoldLive2.delta"
AS
SELECT
  PickupYear,
  PickupMonth,
  PickupDay,
  COUNT(RideId) AS TotalRides,
  SUM(TripDistance) AS TotalDistance,
  SUM(TotalAmount) AS TotalAmount
FROM
  live.YellowTaxis_SilverLiveIncremental
GROUP BY
  PickupYear,
  PickupMonth,
  PickupDay;

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_SummaryByZone_GoldLive
LOCATION "dbfs:/mnt/datalake/Output/YellowTaxis_SummaryByZone_GoldLive.delta"
AS
SELECT
  Zone,
  Borough,
  COUNT(RideId) AS TotalRides,
  SUM(TripDistance) AS TotalDistance,
  SUM(TotalAmount) AS TotalAmount
FROM
  live.YellowTaxis_SilverLiveIncremental yt
  JOIN live.TaxiZones_SilverLive tz ON yt.PickupLocationId = tz.LocationId
GROUP BY
  Zone,
  Borough;

-- COMMAND ----------


