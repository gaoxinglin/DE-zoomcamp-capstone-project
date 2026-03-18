/* @bruin
name: marts.mart_renewable_ratio
type: bq.sql
depends:
  - core.fct_generation
materialization:
  type: table
@bruin */

SELECT
  trading_month,
  SUM(generation_kwh)                                                   AS total_kwh,
  SUM(CASE WHEN fuel_type IN ('Hydro','Geothermal','Wind','Solar')
           THEN generation_kwh ELSE 0 END)                             AS renewable_kwh,
  ROUND(
    100.0 * SUM(CASE WHEN fuel_type IN ('Hydro','Geothermal','Wind','Solar')
                     THEN generation_kwh ELSE 0 END)
    / NULLIF(SUM(generation_kwh), 0), 2
  )                                                                      AS renewable_pct
FROM core.fct_generation
GROUP BY 1
