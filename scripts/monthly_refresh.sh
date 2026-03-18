#!/usr/bin/env bash
# monthly_refresh.sh
# Run on the 5th of each month to download last month's EMI data and rebuild BQ tables.
# Setup: add to crontab with:
#   0 8 5 * * /path/to/project/scripts/monthly_refresh.sh >> /path/to/project/logs/monthly_refresh.log 2>&1

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "========================================"
echo "NZ Electricity monthly refresh started: $(date)"
echo "========================================"

# Load env vars
if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  source "$PROJECT_DIR/.env"
  set +a
else
  echo "ERROR: .env file not found at $PROJECT_DIR/.env"
  exit 1
fi

# Determine last month's first day (works on macOS and Linux)
if date --version >/dev/null 2>&1; then
  # GNU date (Linux)
  TARGET_DATE=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)
else
  # BSD date (macOS)
  TARGET_DATE=$(date -v-1m +%Y-%m-01)
fi

echo "Target month: $TARGET_DATE"
cd "$PROJECT_DIR"

# Step 1: Download CSV
echo "[1/3] Downloading EMI CSV for $TARGET_DATE..."
bruin run assets/raw/download_emi.py --start-date "$TARGET_DATE" --end-date "$TARGET_DATE"

# Step 2: Rebuild external table + staging
echo "[2/3] Rebuilding external table and staging..."
bruin run assets/raw/generation_raw.py       --start-date "$TARGET_DATE" --end-date "$TARGET_DATE"
bruin run assets/staging/stg_generation.asset.sql --start-date "$TARGET_DATE" --end-date "$TARGET_DATE"

# Step 3: Rebuild core + marts in parallel
echo "[3/3] Rebuilding core and marts..."
bruin run assets/core/fct_generation.asset.sql      --start-date "$TARGET_DATE" --end-date "$TARGET_DATE" &
bruin run assets/core/dim_plant.asset.sql            --start-date "$TARGET_DATE" --end-date "$TARGET_DATE" &
wait
bruin run assets/marts/mart_generation_monthly.asset.sql --start-date "$TARGET_DATE" --end-date "$TARGET_DATE" &
bruin run assets/marts/mart_renewable_ratio.asset.sql    --start-date "$TARGET_DATE" --end-date "$TARGET_DATE" &
bruin run assets/marts/mart_plant_ranking.asset.sql      --start-date "$TARGET_DATE" --end-date "$TARGET_DATE" &
bruin run assets/marts/mart_seasonal_pattern.asset.sql   --start-date "$TARGET_DATE" --end-date "$TARGET_DATE" &
wait

echo "========================================"
echo "Monthly refresh completed: $(date)"
echo "========================================"
