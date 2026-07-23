"""
data_extract_no_risk_Valprop.py
-------------------------------------------------------------------------------
ValProp - no-risk card dataset extract.

Runs valprop/card_query.sql against the Competiscan archive (SSH bastion ->
mysql -e), writes the result to no_risk_ValProp_dataset.csv, and uploads that
CSV to s3://valprop-data.

RUN (research env - paramiko / pandas / boto3):
    C:/miniconda3/envs/research/python.exe valprop/data_extract_no_risk_Valprop.py
-------------------------------------------------------------------------------
"""

import sys
import time
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd

# ConnectToDB_VPN_utils / config live at the project root (parent of valprop/).
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from ConnectToDB_VPN_utils import ssh_connect, ssh_disconnect  # noqa: E402

QUERY_PATH   = Path(__file__).resolve().parent / "card_query.sql"
CSV_PATH     = Path(__file__).resolve().parent / "no_risk_ValProp_dataset.csv"
S3_BUCKET    = "valprop-data"
S3_KEY       = CSV_PATH.name
QUERY_TIMEOUT = 600  # seconds


def _run_sql(query: str, timeout: int = QUERY_TIMEOUT) -> pd.DataFrame:
    """Run `query` via `mysql -e` on the SSH bastion.

    Drains stdout AND stderr concurrently (reading stdout to completion before
    touching stderr, as mcp_serverv3._run_sql does, can deadlock paramiko if
    the remote side blocks trying to flush stderr) and enforces a hard
    timeout with a heartbeat, so a slow/stuck query fails loudly instead of
    hanging forever.
    """
    ssh = ssh_connect()
    try:
        command = f'mysql -h {config.mysql_host} -u {config.mysql_user} -p{config.mysql_passwd} -e "{query}"'
        channel = ssh.get_transport().open_session()
        channel.exec_command(command)

        out, err = bytearray(), bytearray()
        start = time.time()
        last_heartbeat = start
        while True:
            got = False
            if channel.recv_ready():
                out += channel.recv(65536)
                got = True
            if channel.recv_stderr_ready():
                err += channel.recv_stderr(65536)
                got = True
            if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                break
            elapsed = time.time() - start
            if elapsed > timeout:
                raise TimeoutError(f"mysql query timed out after {timeout}s")
            if not got:
                if time.time() - last_heartbeat > 15:
                    print(f"  ... still running ({elapsed:.0f}s)")
                    last_heartbeat = time.time()
                time.sleep(0.2)

        err_text = err.decode("utf-8", errors="replace").strip()
        if err_text and "Using a password on the command line" not in err_text:
            print(f"  mysql stderr: {err_text}")

        raw = out.decode("utf-8", errors="replace")
        if not raw.strip():
            return pd.DataFrame()
        return pd.read_csv(StringIO(raw), sep="\t")
    finally:
        ssh_disconnect(ssh)


def main() -> int:
#    query = QUERY_PATH.read_text(encoding="utf-8")
#    # The query runs inside `mysql -e "..."` over an SSH exec_command, i.e.
#    # through a shell. Backticks survive INSIDE double quotes in bash as
#    # command substitution, so they must be escaped or the column-alias
#    # backticks in card_query.sql would blow up the remote shell command.
#    query = query.replace("`", "\\`")

#    print("Running card_query.sql against the archive...")
#    df = _run_sql(query)
#    if df is None or df.empty:
#        print("ERROR: query returned no rows.")
#        return 1
#    print(f"  {len(df)} rows")

#    df.to_csv(CSV_PATH, index=False)
#    print(f"  saved {CSV_PATH}")

    print(f"Uploading to s3://{S3_BUCKET}/{S3_KEY} ...")
    boto3.client("s3").upload_file(str(CSV_PATH), S3_BUCKET, S3_KEY)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
