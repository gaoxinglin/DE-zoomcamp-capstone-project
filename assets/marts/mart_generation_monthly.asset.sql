/* @bruin
name: marts.mart_generation_monthly
type: bq.sql
depends:
  - core.fct_generation
materialization:
  type: table
@bruin */

SELECT
  trading_month,
  fuel_type,
  SUM(generation_kwh)                      AS total_kwh,
  ROUND(SUM(generation_kwh) / 1e6, 4)     AS total_gwh,
  COUNT(DISTINCT gen_code)                 AS active_generators
FROM core.fct_generation
GROUP BY 1, 2
