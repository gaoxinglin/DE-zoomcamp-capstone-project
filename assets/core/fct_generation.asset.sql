/* @bruin
name: core.fct_generation
type: bq.sql
depends:
  - staging.stg_generation
materialization:
  type: table
  partition_by: trading_date
  cluster_by:
    - fuel_type
@bruin */

-- TODO: implement fact table with surrogate key
