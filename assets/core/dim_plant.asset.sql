/* @bruin
name: core.dim_plant
type: bq.sql
depends:
  - staging.stg_generation
materialization:
  type: table
columns:
  - name: gen_code
    type: string
    checks:
      - name: not_null
      - name: unique
  - name: site_code
    type: string
    checks:
      - name: not_null
  - name: fuel_type
    type: string
    checks:
      - name: not_null
@bruin */

-- Step 1: find the latest trading date observed for each generator
WITH latest_date_per_generator AS (
  SELECT
    gen_code,
    MAX(trading_date) AS latest_date
  FROM staging.stg_generation
  GROUP BY gen_code
),

-- Step 2: from records on that latest date, pick one row per generator
-- (ROW_NUMBER handles the rare case where attributes differ within the same day)
latest_state AS (
  SELECT
    s.gen_code,
    s.site_code,
    s.fuel_type,
    s.tech_code,
    ROW_NUMBER() OVER (
      PARTITION BY s.gen_code
      ORDER BY s.trading_date DESC
    ) AS rn
  FROM staging.stg_generation s
  INNER JOIN latest_date_per_generator l
    ON s.gen_code = l.gen_code
    AND s.trading_date = l.latest_date
)

SELECT
  gen_code,
  site_code,
  fuel_type,
  tech_code
FROM latest_state
WHERE rn = 1
