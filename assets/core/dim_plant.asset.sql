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

SELECT DISTINCT
  gen_code,
  site_code,
  fuel_type,
  tech_code
FROM staging.stg_generation
