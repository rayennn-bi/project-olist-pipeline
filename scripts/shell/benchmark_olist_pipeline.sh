#!/usr/bin/env bash
# =============================================================================
#  benchmark_olist_pipeline.sh
#  Benchmark end-to-end pipeline olist_pipeline_v1 dan tampilkan ringkasan
#  lengkap per-task, final row count, dan overall result.
#
#  Usage:
#    bash benchmark_olist_pipeline.sh
#
#  Override via environment variable:
#    DAG_ID=olist_pipeline_v1 MAX_SECONDS=900 bash benchmark_olist_pipeline.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Konfigurasi — semua bisa di-override dari environment
# ---------------------------------------------------------------------------
COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"
DAG_ID="${DAG_ID:-olist_pipeline_v1}"
MAX_SECONDS="${MAX_SECONDS:-900}"
POLL_INTERVAL="${POLL_INTERVAL:-15}"
RUN_ID="${RUN_ID:-benchmark_$(date +%Y%m%dT%H%M%S)}"

# Tabel final yang divalidasi dan minimum row target
FINAL_TABLE="${FINAL_TABLE:-warehouse.fact_sales_enriched}"
MIN_ROWS="${MIN_ROWS:-100000}"

# ---------------------------------------------------------------------------
# Warna output
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
log_info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

require_service() {
  local service="$1"
  if [[ -z "$(${COMPOSE_CMD} ps -q "${service}" 2>/dev/null)" ]]; then
    log_error "Service '${service}' tidak berjalan. Jalankan 'make up' atau 'docker compose up -d' terlebih dahulu."
    exit 1
  fi
  log_ok "Service '${service}' aktif"
}

# Query ke database AIRFLOW — untuk metadata dag_run & task_instance
psql_airflow() {
  local query="$1"
  ${COMPOSE_CMD} exec -T postgres \
    psql -U airflow -d airflow -t -A -c "${query}" | tr -d '[:space:]'
}

# Query ke database OLIST_DW — untuk row count tabel warehouse
psql_dw() {
  local query="$1"
  ${COMPOSE_CMD} exec -T postgres \
    psql -U airflow -d olist_dw -t -A -c "${query}" | tr -d '[:space:]'
}

get_dag_state() {
  psql_airflow \
    "SELECT COALESCE(state, '')
     FROM dag_run
     WHERE dag_id = '${DAG_ID}'
       AND run_id  = '${RUN_ID}'
     ORDER BY execution_date DESC
     LIMIT 1;"
}

get_task_durations() {
  ${COMPOSE_CMD} exec -T postgres \
    psql -U airflow -d airflow -t -A -F '|' -c \
    "SELECT task_id,
            COALESCE(
              EXTRACT(EPOCH FROM (end_date - start_date))::int,
              0
            ) AS duration_seconds,
            COALESCE(state, 'unknown') AS state
     FROM task_instance
     WHERE dag_id = '${DAG_ID}'
       AND run_id  = '${RUN_ID}'
     ORDER BY start_date ASC NULLS LAST;"
}

get_row_count() {
  local table="$1"
  psql_dw "SELECT COUNT(*) FROM ${table};"
}

format_duration() {
  local total_seconds="$1"
  printf "%02dm %02ds" "$((total_seconds / 60))" "$((total_seconds % 60))"
}

pass_fail() {
  if [[ "$1" == "PASS" ]]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
echo
echo -e "${BOLD}================================================${NC}"
echo -e "${BOLD}   OLIST PIPELINE BENCHMARK${NC}"
echo -e "${BOLD}================================================${NC}"
echo

log_info "Memeriksa services yang dibutuhkan..."
require_service "airflow-webserver"
require_service "postgres"
echo

log_info "Benchmark configuration"
echo "       DAG_ID         : ${DAG_ID}"
echo "       RUN_ID         : ${RUN_ID}"
echo "       FINAL_TABLE    : ${FINAL_TABLE}"
echo "       MIN_ROWS       : ${MIN_ROWS}"
echo "       MAX_SECONDS    : ${MAX_SECONDS}"
echo "       POLL_INTERVAL  : ${POLL_INTERVAL}"
echo

# ---------------------------------------------------------------------------
# Trigger DAG
# ---------------------------------------------------------------------------
log_info "Triggering DAG '${DAG_ID}'..."
${COMPOSE_CMD} exec -T airflow-webserver \
  airflow dags trigger --run-id "${RUN_ID}" "${DAG_ID}" \
  >/dev/null 2>&1
log_ok "DAG berhasil di-trigger"
echo

# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------
start_ts="$(date +%s)"
state="queued"

log_info "Memantau status DAG setiap ${POLL_INTERVAL}s..."
echo

while true; do
  sleep "${POLL_INTERVAL}"

  state="$(get_dag_state || echo '')"
  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"

  if [[ -z "${state}" ]]; then
    log_warn "DAG run belum terlihat di metadata, terus memantau..."
    continue
  fi

  log_info "Current DAG state: ${BOLD}${state}${NC} | elapsed=$(format_duration "${elapsed}")"

  if (( elapsed >= MAX_SECONDS )) && [[ "${state}" != "success" && "${state}" != "failed" ]]; then
    log_error "Timeout ${MAX_SECONDS}s tercapai — pipeline dianggap gagal."
    state="timeout"
    break
  fi

  case "${state}" in
    success|failed) break ;;
  esac
done

end_ts="$(date +%s)"
total_elapsed="$((end_ts - start_ts))"
echo

# ---------------------------------------------------------------------------
# Kumpulkan statistik task per-task
# ---------------------------------------------------------------------------
declare -a task_rows=()
slowest_task=""; slowest_secs=0
failed_tasks=0

while IFS='|' read -r task_id duration_secs task_state; do
  [[ -z "${task_id}" ]] && continue
  task_rows+=("${task_id}|${duration_secs}|${task_state}")
  if [[ "${task_state}" == "failed" ]]; then
    (( failed_tasks++ )) || true
  fi
  if (( duration_secs > slowest_secs )); then
    slowest_secs="${duration_secs}"
    slowest_task="${task_id}"
  fi
done < <(get_task_durations || true)

# ---------------------------------------------------------------------------
# Validasi final table row count
# ---------------------------------------------------------------------------
final_row_count="$(get_row_count "${FINAL_TABLE}" 2>/dev/null || echo '0')"
if [[ ! "${final_row_count}" =~ ^[0-9]+$ ]]; then
  final_row_count=0
fi

if (( final_row_count >= MIN_ROWS )); then
  pass_rows="PASS"
else
  pass_rows="FAIL"
fi

# ---------------------------------------------------------------------------
# Evaluasi overall result
# ---------------------------------------------------------------------------
pass_runtime="FAIL"
pass_state="FAIL"
overall="FAIL"

if (( total_elapsed <= MAX_SECONDS )); then pass_runtime="PASS"; fi
if [[ "${state}" == "success" ]];      then pass_state="PASS";   fi

if [[ "${pass_state}" == "PASS" && "${pass_runtime}" == "PASS" && "${pass_rows}" == "PASS" ]]; then
  overall="PASS"
fi

# ---------------------------------------------------------------------------
# Cetak ringkasan
# ---------------------------------------------------------------------------
SEP="────────────────────────────────────────────────────────────"

echo
echo -e "${BOLD}${SEP}${NC}"
echo -e "${BOLD}  Benchmark Summary${NC}"
echo -e "${BOLD}${SEP}${NC}"
printf "  %-22s : %s\n" "DAG ID"          "${DAG_ID}"
printf "  %-22s : %s\n" "Run ID"          "${RUN_ID}"
printf "  %-22s : %s\n" "Final table"     "${FINAL_TABLE}"
printf "  %-22s : %s\n" "DAG state"       "${state}"
printf "  %-22s : %s (%ss)\n" "Elapsed time" "$(format_duration "${total_elapsed}")" "${total_elapsed}"
printf "  %-22s : %s\n" "Final row count" "${final_row_count}"
printf "  %-22s : %s -> " "Row target"    "${MIN_ROWS}"
echo -e "$(pass_fail "${pass_rows}")"
printf "  %-22s : %ss -> " "Time target"  "${MAX_SECONDS}"
echo -e "$(pass_fail "${pass_runtime}")"

echo
echo -e "${BOLD}  Task Duration Breakdown${NC}"
echo -e "  ${SEP:0:55}"
printf "  %-45s %10s  %s\n" "TASK ID" "DURATION" "STATE"
echo -e "  ${SEP:0:55}"
for row in "${task_rows[@]}"; do
  IFS='|' read -r tid tdur tstate <<< "${row}"
  if   [[ "${tstate}" == "success" ]]; then state_colored="${GREEN}${tstate}${NC}"
  elif [[ "${tstate}" == "failed"  ]]; then state_colored="${RED}${tstate}${NC}"
  else state_colored="${YELLOW}${tstate}${NC}"
  fi
  printf "  %-45s %10s  " "${tid}" "$(format_duration "${tdur}")"
  echo -e "${state_colored}"
done
echo -e "  ${SEP:0:55}"
if [[ -n "${slowest_task}" ]]; then
  printf "  Slowest task: ${YELLOW}%s${NC} (%s)\n" "${slowest_task}" "$(format_duration "${slowest_secs}")"
fi
printf "  Failed tasks: "
if (( failed_tasks > 0 )); then
  echo -e "${RED}${failed_tasks}${NC}"
else
  echo -e "${GREEN}0${NC}"
fi

echo
echo -e "${BOLD}${SEP}${NC}"
printf "  %-22s : " "Overall result"
echo -e "${BOLD}$(pass_fail "${overall}")${NC}"
echo -e "${BOLD}${SEP}${NC}"
echo

# ---------------------------------------------------------------------------
# Exit code
# ---------------------------------------------------------------------------
if [[ "${overall}" != "PASS" ]]; then
  exit 1
fi