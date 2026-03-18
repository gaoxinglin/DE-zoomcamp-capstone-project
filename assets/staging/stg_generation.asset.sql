/* @bruin
name: staging.stg_generation
type: bq.sql
depends:
  - raw.generation_raw
materialization:
  type: table
  partition_by: trading_date
  cluster_by:
    - fuel_type
columns:
  - name: trading_date
    type: date
    checks:
      - name: not_null
  - name: trading_period
    type: integer
    checks:
      - name: not_null
      - name: min
        value: 1
      - name: max
        value: 50
  - name: gen_code
    type: string
    checks:
      - name: not_null
  - name: fuel_type
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value: ["Hydro","Geothermal","Gas","Wind","Coal","Solar","Diesel","Wood","Battery","Unknown"]
  - name: generation_kwh
    type: float
    checks:
      - name: not_null
      - name: non_negative
custom_checks:
  - name: row_count_sanity
    description: "At least 1000 rows per run"
    query: "SELECT IF(COUNT(*) < 1000, 1, 0) FROM staging.stg_generation"
@bruin */

WITH unpivoted AS (
  SELECT
    Site_Code    AS site_code,
    POC_Code     AS poc_code,
    Nwk_Code     AS nwk_code,
    Gen_Code     AS gen_code,
    Fuel_Code    AS raw_fuel_code,
    Tech_Code    AS tech_code,
    PARSE_DATE('%Y-%m-%d', Trading_Date) AS trading_date,
    tp_name,
    generation_kwh
  FROM raw.generation_raw
  UNPIVOT (
    generation_kwh FOR tp_name IN (
      TP1,  TP2,  TP3,  TP4,  TP5,  TP6,  TP7,  TP8,  TP9,  TP10,
      TP11, TP12, TP13, TP14, TP15, TP16, TP17, TP18, TP19, TP20,
      TP21, TP22, TP23, TP24, TP25, TP26, TP27, TP28, TP29, TP30,
      TP31, TP32, TP33, TP34, TP35, TP36, TP37, TP38, TP39, TP40,
      TP41, TP42, TP43, TP44, TP45, TP46, TP47, TP48, TP49, TP50
    )
  )
  WHERE generation_kwh IS NOT NULL
    AND generation_kwh >= 0
)

SELECT
  site_code,
  poc_code,
  nwk_code,
  gen_code,
  tech_code,
  trading_date,
  CAST(REGEXP_EXTRACT(tp_name, r'TP(\d+)') AS INT64) AS trading_period,
  generation_kwh,
  CASE UPPER(TRIM(raw_fuel_code))
    WHEN 'HYDRO'      THEN 'Hydro'
    WHEN 'HYD'        THEN 'Hydro'
    WHEN 'GEO'        THEN 'Geothermal'
    WHEN 'GEOTHERMAL' THEN 'Geothermal'
    WHEN 'GAS'        THEN 'Gas'
    WHEN 'WIND'       THEN 'Wind'
    WHEN 'COAL'       THEN 'Coal'
    WHEN 'SOLAR'      THEN 'Solar'
    WHEN 'SOL'        THEN 'Solar'
    WHEN 'DIESEL'     THEN 'Diesel'
    WHEN 'WOOD'       THEN 'Wood'
    WHEN 'ELE'        THEN 'Battery'
    ELSE 'Unknown'
  END AS fuel_type,
  FORMAT_DATE('%Y%m', trading_date) AS trading_month
FROM unpivoted
