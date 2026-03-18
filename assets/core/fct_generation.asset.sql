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
columns:
  - name: generation_id
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: generation_kwh
    type: float
    checks:
      - name: non_negative
@bruin */

WITH deduped AS (
  SELECT
    trading_date,
    trading_period,
    site_code,
    gen_code,
    fuel_type,
    tech_code,
    generation_kwh,
    trading_month,
    ROW_NUMBER() OVER (
      PARTITION BY trading_date, trading_period, gen_code
      ORDER BY generation_kwh DESC
    ) AS rn
  FROM staging.stg_generation
)

SELECT
  TO_HEX(MD5(CONCAT(
    CAST(trading_date    AS STRING), '|',
    CAST(trading_period  AS STRING), '|',
    gen_code
  )))            AS generation_id,
  trading_date,
  trading_period,
  site_code,
  gen_code,
  fuel_type,
  tech_code,
  generation_kwh,
  trading_month
FROM deduped
WHERE rn = 1
