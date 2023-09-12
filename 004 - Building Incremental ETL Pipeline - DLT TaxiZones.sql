-- Databricks notebook source
-- Read From COMPLETE     ->  Write to COMPLETE     ->  Data is overwritten
-- Read From COMPLETE     ->  Write to INCREMENTAL  ->  Not Possible
-- Read From INCREMENTAL  ->  Write to COMPLETE     ->  Data is overwritten
-- Read From INCREMENTAL  ->  Write to INCREMENTAL  ->  Movement of new records

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (A) Create Live Bronze Table

-- COMMAND ----------

CREATE LIVE TABLE TaxiZones_BronzeLive
LOCATION "dbfs:/mnt/datalake/Output/TaxiZones_BronzeLive.delta"
COMMENT "Live Bronze table for Taxi Zones"
AS
SELECT
  *
FROM
  parquet.`dbfs:/mnt/raw/deep_dive/TaxiZones.parquet`;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### (B) Create Live Bronze View
-- MAGIC
-- MAGIC <p>We can define CONSTRAINTS on a View like we did for the table<\p>

-- COMMAND ----------

CREATE LIVE VIEW TaxiZones_SilverLive (
  CONSTRAINT Valid_LocationId EXPECT (
    LocationId IS NOT NULL
    AND LocationId > 0
  ) ON VIOLATION DROP ROW
)
COMMENT "Live Bronze view for Taxi Zones" AS
SELECT
  *
FROM
  live.TaxiZones_BronzeLive;

-- COMMAND ----------


