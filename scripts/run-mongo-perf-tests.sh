#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_PATH="$ROOT_DIR/src/NStore.Persistence.Mongo.Tests/NStore.Persistence.Mongo.Tests.csproj"
RESULTS_DIR="$ROOT_DIR/TestResults"
CONFIGURATION="${CONFIGURATION:-Release}"
TARGET_FRAMEWORK="${TARGET_FRAMEWORK:-net10.0}"

if [ ! -f "$PROJECT_PATH" ]; then
  echo "Project file not found: $PROJECT_PATH" >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR"

TMP_ROWS="$(mktemp)"
trap 'rm -f "$TMP_ROWS"' EXIT

file_mtime_epoch() {
  local file="$1"
  if stat -f %m "$file" >/dev/null 2>&1; then
    stat -f %m "$file"
  else
    stat -c %Y "$file"
  fi
}

latest_suite_file() {
  local prefix="$1"
  local build_results_dir
  build_results_dir="$ROOT_DIR/src/NStore.Persistence.Mongo.Tests/bin/$CONFIGURATION/$TARGET_FRAMEWORK/TestResults"

  ls -1t \
    "$RESULTS_DIR"/"$prefix"-*/suite.csv \
    "$build_results_dir"/"$prefix"-*/suite.csv \
    2>/dev/null | head -n 1 || true
}

append_rows_from_suite() {
  local mode="$1"
  local suite_file="$2"

  awk -F',' -v mode="$mode" '
    BEGIN { OFS="\t" }
    /^#/ { next }
    /^scenario_name,/ { next }
    NF == 0 { next }
    {
      scenario=$1
      batch=$2
      writers=$3
      effective_writers=$4
      elapsed_s=$8
      throughput=$9
      median_ms=$10
      worst_degradation=$13

      gsub(/^"|"$/, "", scenario)
      gsub(/^"|"$/, "", worst_degradation)

      print mode, scenario, batch, writers, effective_writers, elapsed_s, throughput, median_ms, worst_degradation
    }
  ' "$suite_file" >> "$TMP_ROWS"
}

run_single_perf_test() {
  local mode="$1"
  local suite_prefix="$2"
  local filter="$3"

  echo "Running $mode perf test..."
  local started_at
  started_at="$(date +%s)"

  NStore__Mongo__Performance__Enabled=true \
    dotnet test "$PROJECT_PATH" \
    -c "$CONFIGURATION" \
    -f "$TARGET_FRAMEWORK" \
    --disable-build-servers \
    --blame-hang-timeout 10m \
    --filter "$filter"

  local suite_file
  suite_file="$(latest_suite_file "$suite_prefix")"
  if [ -z "$suite_file" ] || [ "$(file_mtime_epoch "$suite_file")" -lt "$started_at" ]; then
    echo "No suite.csv produced for $mode." >&2
    echo "Perf mode should be enabled by script via NStore__Mongo__Performance__Enabled=true." >&2
    exit 1
  fi

  append_rows_from_suite "$mode" "$suite_file"
}

run_single_perf_test \
  "channel-workers" \
  "channel-workers" \
  "FullyQualifiedName~mongodb_batch_insert_performance_tests.should_measure_batch_insert_performance_degradation"

run_single_perf_test \
  "extension-method" \
  "extension-method" \
  "FullyQualifiedName~mongodb_parallel_extension_batch_insert_performance_tests.should_measure_parallel_extension_batch_insert_performance_degradation"

if [ ! -s "$TMP_ROWS" ]; then
  echo "No scenario rows found in suite output." >&2
  exit 1
fi

echo
echo "Final Results"
printf "%-18s %-40s %8s %8s %8s %10s %14s %12s %12s\n" \
  "mode" "scenario" "batch" "writers" "eff_wrt" "elapsed_s" "items_per_sec" "median_ms" "worst_deg_x"
printf "%-18s %-40s %8s %8s %8s %10s %14s %12s %12s\n" \
  "------------------" "----------------------------------------" "--------" "--------" "--------" "----------" "--------------" "------------" "------------"

awk -F'\t' '
  {
    printf "%-18s %-40s %8s %8s %8s %10s %14s %12s %12s\n",
      $1, $2, $3, $4, $5, $6, $7, $8, $9
  }
' "$TMP_ROWS"
