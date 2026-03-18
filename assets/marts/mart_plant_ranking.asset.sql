/* @bruin
name: marts.mart_plant_ranking
type: bq.sql
depends:
  - core.fct_generation
materialization:
  type: table
@bruin */

SELECT
  trading_month,
  site_code,
  fuel_type,
  SUM(generation_kwh)                                AS total_kwh,
  RANK() OVER (
    PARTITION BY trading_month
    ORDER BY SUM(generation_kwh) DESC
  )                                                  AS monthly_rank
FROM core.fct_generation
GROUP BY 1, 2, 3
