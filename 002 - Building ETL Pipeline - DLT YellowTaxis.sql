-- Databricks notebook source
-- Steps to create ETL pipeline:
-- ----------------------------

--   1.  Create live datasets
--       -   Delta tables or views
--       -   Types - Complete, Incremental & Streaming

--   2.  Define data quality checks (expectations) on datasets
--       -   Constraints to apply
--       -   Actions in case of errors

--   3.  Define data transformation queries
--       -   Business logic to clean, filter, transform, aggregate
--       -   Define inter-dependencies between datasets

--   4.  Create, run & test pipelines
--       -   Auto-manage infra, process data, maintain lineage, apply quality checks, handle logs, retry on failures, etc
--       - Mode of execution - Triggered and Continuous

--   5.  Promote to production

-- CREATE LIVE TABLE is Complete Table mode. If you run the pipeline again, all data in the tables will be overwritten
--    If you upload another file to the location "dbfs:/mnt/raw/deep_dive/YellowTaxisParquet" and run the pipeline again, it will pickup both the files. That is, it will reprocess the first file again

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (A) Create Live Bronze Table
-- MAGIC
-- MAGIC <p>It is not different from regular Delta table. "LIVE" means it will be managed by DLT<\p>
-- MAGIC
-- MAGIC <p>For a LIVE table it is mandatory to define the SELECT statement. This means how the data will be populated into this table<\p>
-- MAGIC
-- MAGIC <p>Since Bronze tables keep raw data, so we are not adding any transformations<\p>

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_BronzeLive (
  RideId INT COMMENT 'this is the primary key column',
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
LOCATION '/mnt/datalake/Output/YellowTaxis_BronzeLive.delta'
PARTITIONED BY (VendorId)
COMMENT 'Live Bronze table for YellowTaxis'
AS
SELECT
  *,
  INPUT_FILE_NAME() AS FileName,
  CURRENT_TIMESTAMP() AS CreatedOn
FROM
  parquet.`dbfs:/mnt/raw/deep_dive/YellowTaxisParquet`;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Live Silver Table
-- MAGIC
-- MAGIC <p>This is also a Live table and you can define this like a regular Delta table<\p>
-- MAGIC
-- MAGIC <p>It has lesser columns and it has some generated columns as well<\p>
-- MAGIC
-- MAGIC <p>You can define Data Quality Checks or Expectations<\p>

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_SilverLive (
  RideId INT COMMENT "This is the primary key column",
  VendorId INT,
  PickupTime TIMESTAMP,
  DropTime TIMESTAMP,
  PickupLocationId INT,
  DropLocationId INT,
  TripDistance DOUBLE,
  TotalAmount DOUBLE,
  PickupYear INT GENERATED ALWAYS AS (YEAR(PickupTime)),
  PickupMonth INT GENERATED ALWAYS AS (MONTH(PickupTime)),
  PickupDay INT GENERATED ALWAYS AS (DAY(PickupTime)),
  CreatedOn TIMESTAMP,
  CONSTRAINT Valid_TotalAmount EXPECT(
    TotalAmount IS NOT NULL
    AND TotalAmount > 0
  ) ON VIOLATION DROP ROW,
  CONSTRAINT Valid_TripDistance EXPECT(TripDistance > 0) ON VIOLATION DROP ROW,
  CONSTRAINT Valid_RideId EXPECT(
    RideId IS NOT NULL
    AND RideId > 0
  ) ON VIOLATION FAIL
  UPDATE
)
USING DELTA
LOCATION "/mnt/datalake/Output/YellowTaxis_SilverLive.delta"
PARTITIONED BY (PickupLocationId)
AS
SELECT
  RideId,
  VendorId,
  PickupTime,
  DropTime,
  PickupLocationId,
  DropLocationId,
  TripDistance,
  TotalAmount,
  CURRENT_TIMESTAMP() AS CreatedOn
FROM
  live.YellowTaxis_BronzeLive;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (C) Create Live Gold Table - 1
-- MAGIC
-- MAGIC <p>We are not defining any columns here. It will take the schema from the query<\p>

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_SummaryByLocation_GoldLive
LOCATION "/mnt/datalake/Output/YellowTaxis_SummaryByLocation_GoldLive.delta"
AS
SELECT
  PickupLocationId,
  DropLocationId,
  COUNT(RideId) AS TotalRides,
  SUM(TripDistance) AS TotalDistance,
  SUM(TotalAmount) AS TotalAmount
FROM
  live.YellowTaxis_SilverLive
GROUP BY
  PickupLocationId,
  DropLocationId;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (D) Create Live Gold Table - 2
-- MAGIC

-- COMMAND ----------

CREATE LIVE TABLE YellowTaxis_SummaryByDate_GoldLive
LOCATION "/mnt/datalake/Output/YellowTaxis_SummaryByDate_GoldLive.delta"
AS
SELECT
  PickupYear,
  PickupMonth,
  PickupDay,
  COUNT(RideId) AS TotalRides,
  SUM(TripDistance) AS TotalDistance,
  SUM(TotalAmount) AS TotalAmount
FROM
  live.YellowTaxis_SilverLive
GROUP BY
  PickupYear,
  PickupMonth,
  PickupDay;

-- COMMAND ----------


