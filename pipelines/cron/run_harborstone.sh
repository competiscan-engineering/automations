#!/usr/bin/env bash
# Cron entry point for the Harborstone weekly deck.
# Schedule: Monday mornings (see crontab.example) — report_HarborstoneWeekly.py's
# own _week_window() default already picks the most recently COMPLETED
# Mon->Mon week with no env vars needed, so this wrapper passes no overrides.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/common.sh"

export HARBOR_EMAIL_TO="hgquijano@competiscan.com"

run_report "pipelines/report_HarborstoneWeekly.py" "harborstone"
exit $?
