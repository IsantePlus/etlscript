#!/bin/bash
user=$1;
pass=$2;
host=$3;
port=$4;

LOG_DIR="/var/log/isanteplus-etl"
mkdir -p "${LOG_DIR}" 2>/dev/null
LOG_FILE="${LOG_DIR}/load-$(date +%Y%m%d-%H%M%S).log"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "${LOG_FILE}"
}

run_sql() {
    local script=$1
    local name=$(basename "$script")
    log "Running ${name}..."
    mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} --force --show-warnings -vv < "$script" >>"${LOG_FILE}" 2>&1
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log "ERROR: ${name} failed with exit code ${exit_code}"
    else
        log "OK: ${name} completed"
    fi
}

log "=== iSantePlus ETL load started ==="
run_sql ./sql_files/isanteplusreportsddlscript.sql
run_sql ./sql_files/isanteplusreportsdmlscript.sql
run_sql ./sql_files/drug_lookup_isanteplus.sql
run_sql ./sql_files/run_isante_patient_status.sql
run_sql ./sql_files/insertion_obs_by_day.sql
run_sql ./sql_files/patient_status_arv_dml.sql
run_sql ./sql_files/indicators_report.sql
log "=== iSantePlus ETL load finished ==="
log "Log file: ${LOG_FILE}"
