#!/usr/bin/env bash
# Cron entry point for the SupplyHouse.com competitor-ads report.
# Schedule: early on the 1st of each month (see crontab.example) —
# report_SupplyHouseCompetitors.py's _month_window() default is the PRIOR
# calendar month, so firing just after month-end reports on the month that
# just finished (fully complete data, not a partial in-progress month).
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/common.sh"

export SH_EMAIL_TO="hgquijano@competiscan.com"

run_report "pipelines/report_SupplyHouseCompetitors.py" "supplyhouse"
exit $?
