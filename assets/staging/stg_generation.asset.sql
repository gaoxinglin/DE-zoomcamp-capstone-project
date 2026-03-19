/* @bruin

name: staging.stg_generation
type: bq.sql
description: Transforms raw NZ electricity generation data from wide-format EMI CSV files into normalized trading period records. Unpivots 50 trading periods (TP1-TP50) into individual rows and standardizes inconsistent fuel codes from EMI data source. This staging table serves as the foundation for dimensional modeling in core layer, providing clean, structured generation data partitioned by trading date and clustered by fuel type for efficient querying.
tags:
  - domain:energy
  - layer:staging
  - data_type:fact_table
  - source:emi_nz
  - update_pattern:monthly_append
  - governance:partitioned

materialization:
  type: table
  strategy: delete+insert
  incremental_key: trading_date
  partition_by: trading_date
  cluster_by:
    - fuel_type

depends:
  - raw.generation_raw

columns:
  - name: trading_date
    type: DATE
    description: NZ electricity market trading date (calendar date). Each trading date contains 50 trading periods of ~30 minutes each, covering a full 24-hour period.
    checks:
      - name: not_null
  - name: trading_period
    type: INTEGER
    description: 'Trading period within the day (1-50). Each period represents ~30 minutes: TP1 = 00:00-00:30, TP2 = 00:30-01:00, etc. Derived from unpivoting TP1-TP50 columns in source data.'
    checks:
      - name: not_null
      - name: min
        value: 1
      - name: max
        value: 50
  - name: gen_code
    type: STRING
    description: Unique generator code assigned by Electricity Authority. Primary identifier for individual generation units (e.g., TAU2201, HLY2201). Combines with trading_date + trading_period to form natural key.
    checks:
      - name: not_null
  - name: fuel_type
    type: STRING
    description: Standardized fuel/technology type for electricity generation. Transformed from inconsistent EMI fuel codes (e.g., 'HYD'→'Hydro', 'ELE'→'Battery'). Used for renewable vs non-renewable classification in downstream marts.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Hydro
          - Geothermal
          - Gas
          - Wind
          - Coal
          - Solar
          - Diesel
          - Wood
          - Battery
          - Unknown
  - name: generation_kwh
    type: FLOAT
    description: Actual electricity generation output in kilowatt-hours (kWh) for this specific trading period. Source data filtered to exclude null/negative values during UNPIVOT transformation.
    checks:
      - name: not_null
      - name: non_negative
  - name: site_code
    type: STRING
    description: Physical generation site identifier (e.g., 'TAU' for Taupo, 'HLY' for Huntly). Multiple generators can exist at the same site. Used in downstream dimensional modeling.
  - name: poc_code
    type: STRING
    description: Point of Connection code in the national grid. Technical identifier for grid connection point, primarily used for network analysis and grid management.
  - name: nwk_code
    type: STRING
    description: Network code indicating the regional grid network. Part of EMI's grid topology classification system, used for transmission and distribution analysis.
  - name: tech_code
    type: STRING
    description: Technology classification code from EMI data. Additional granularity beyond fuel_type for specific generation technologies and configurations.
  - name: trading_month
    type: STRING
    description: Derived field in YYYYMM format (e.g., '202401') for convenient monthly aggregations. Used extensively in mart layer for time-series analysis and monthly reporting.
    checks:
      - name: not_null

custom_checks:
  - name: row_count_sanity
    description: Ensures minimum viable data volume per pipeline run. Monthly EMI files should contain thousands of generation records across ~87 active generators and 50 trading periods per day.
    value: 0
    query: SELECT IF(COUNT(*) < 1000, 1, 0) FROM staging.stg_generation
  - name: all_trading_periods_present
    description: Validates that all 50 trading periods (TP1-TP50) are represented in the data, ensuring no systematic gaps in EMI source files.
    value: 0
    query: SELECT IF(COUNT(DISTINCT trading_period) < 50, 1, 0) FROM staging.stg_generation
  - name: fuel_type_standardization
    description: Confirms that fuel type mapping logic successfully handles all EMI fuel codes, with minimal 'Unknown' classifications.
    value: 0
    query: SELECT IF(COUNTIF(fuel_type = 'Unknown') / COUNT(*) > 0.01, 1, 0) FROM staging.stg_generation

@bruin */

-- BigQuery's UNPIVOT requires all column names to be listed explicitly;
-- dynamic column lists are not supported in standard SQL, so all 50 TP
-- columns must be enumerated here. Null and negative values are filtered
-- out at this stage so downstream layers only ever see valid readings.
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
  WHERE PARSE_DATE('%Y-%m-%d', Trading_Date) BETWEEN DATE('{{start_date}}') AND DATE('{{end_date}}')
    AND generation_kwh IS NOT NULL
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
