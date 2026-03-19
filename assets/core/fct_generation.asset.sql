/* @bruin
name: core.fct_generation
type: bq.sql
depends:
  - staging.stg_generation
materialization:
  type: table
  strategy: delete+insert
  incremental_key: trading_date
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

-- Deduplicate staging records: the same (date, period, gen_code) can appear
-- more than once when EMI republishes a corrected file for a past month.
-- We keep the higher-generation row as the authoritative value.
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
  WHERE trading_date BETWEEN DATE('{{start_date}}') AND DATE('{{end_date}}')
)

SELECT
  -- Surrogate key: MD5 over pipe-delimited fields prevents hash collisions
  -- that would occur if fields were concatenated without a separator
  -- (e.g. date="2024-01-1", period=2 vs date="2024-01-12", period=blank).
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
