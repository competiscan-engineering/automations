#!/usr/bin/env bash
# Shared helper for the report_* cron wrappers in this directory — sourced,
# never executed directly. Edit the two paths below once for this VM.

PROJECT_ROOT="/opt/automations"
CONDA_PYTHON="/opt/miniconda3/envs/research/bin/python"   # python.exe inside the "research" conda env
LOG_DIR="$PROJECT_ROOT/pipelines/output/logs"
mkdir -p "$LOG_DIR"

# run_report <script path, relative to PROJECT_ROOT> <log file prefix>
# Runs the pipeline, appends timestamped stdout/stderr to a log file under
# LOG_DIR, prunes logs older than 90 days, and returns the pipeline's exit
# code. A non-zero exit is also echoed to stderr so cron's own mail (if MAILTO
# is set) surfaces the failure immediately, not just on the next manual log check.
run_report() {
    local script="$1" log_prefix="$2"
    local stamp log_file status
    stamp="$(date +%Y%m%d_%H%M%S)"
    log_file="$LOG_DIR/${log_prefix}_${stamp}.log"

    cd "$PROJECT_ROOT" || return 1
    {
        echo "=== ${log_prefix} start: $(date -Is) ==="
        "$CONDA_PYTHON" "$script"
        status=$?
        echo "=== ${log_prefix} end:   $(date -Is)  (exit ${status}) ==="
    } >>"$log_file" 2>&1

    find "$LOG_DIR" -name "${log_prefix}_*.log" -mtime +90 -delete 2>/dev/null

    if [ "$status" -ne 0 ]; then
        echo "${log_prefix} FAILED (exit ${status}) — see ${log_file}" >&2
    fi
    return "$status"
}
