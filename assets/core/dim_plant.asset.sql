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

-- Keep only the most recently observed state per generator
WITH latest AS (
  SELECT
    gen_code,
    site_code,
    fuel_type,
    tech_code,
    ROW_NUMBER() OVER (
      PARTITION BY gen_code
      ORDER BY MAX(trading_date) DESC
    ) AS rn
  FROM staging.stg_generation
  GROUP BY gen_code, site_code, fuel_type, tech_code
)

SELECT
  gen_code,
  site_code,
  fuel_type,
  tech_code
FROM latest
WHERE rn = 1
