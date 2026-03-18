/* @bruin
name: marts.mart_seasonal_pattern
type: bq.sql
depends:
  - core.fct_generation
materialization:
  type: table
@bruin */

-- NZ seasons (Southern Hemisphere)
SELECT
  CASE
    WHEN EXTRACT(MONTH FROM trading_date) IN (12, 1, 2) THEN 'Summer'
    WHEN EXTRACT(MONTH FROM trading_date) IN (3, 4, 5)  THEN 'Autumn'
    WHEN EXTRACT(MONTH FROM trading_date) IN (6, 7, 8)  THEN 'Winter'
    ELSE 'Spring'
  END                                    AS season,
  fuel_type,
  SUM(generation_kwh)                    AS total_kwh,
  ROUND(SUM(generation_kwh) / 1e6, 4)  AS total_gwh,
  COUNT(DISTINCT trading_date)          AS days
FROM core.fct_generation
GROUP BY 1, 2
