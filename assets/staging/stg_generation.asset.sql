/* @bruin
name: staging.stg_generation
type: bq.sql
depends:
  - raw.generation_raw
materialization:
  type: table
@bruin */

-- TODO: implement UNPIVOT + fuel_type standardization
