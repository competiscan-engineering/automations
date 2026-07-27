#!/usr/bin/env bash
# Cron entry point for the Monthly Banking Merger report.
# Schedule: the LAST day of each month, late evening (see crontab.example's
# date-guard) — report_MonthlyBankingMerger.py always reports on
# datetime.now()'s current month, so firing on the month's final day is what
# makes "current month" mean "the complete month," per how this was scoped.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/common.sh"

export MERGER_EMAIL_TO="hgquijano@competiscan.com"

run_report "pipelines/report_MonthlyBankingMerger.py" "merger"
exit $?
