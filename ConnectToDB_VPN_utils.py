import os
import sys
import subprocess
import time
import pathlib
import traceback
import paramiko
from io import StringIO
import pandas as pd
from mysql.connector import Error
import config
from pathlib import Path

mysql_host = config.mysql_host
mysql_user = config.mysql_user
mysql_passwd = config.mysql_passwd

# CONFIG
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OVPN_CONF = os.path.join(BASE_DIR, "jose.ignacio.ovpn")
OVPN_PID  = "competiscan-openvpn.pid"
TUN_IFACE = "tun0"
READY_MARK = "Initialization Sequence Completed"
WAIT_TIMEOUT = 120

SSH_HOST = "54.149.124.83"         
SSH_USER = "ubuntu"
SSH_KEY  = os.path.join(BASE_DIR, "python-server.pem")
SSH_PORT = 22
VPN_GW=None

def run(cmd, check=True, capture=False):
    return subprocess.run(
        cmd, text=True, check=check,
        stdout=(subprocess.PIPE if capture else None),
        stderr=(subprocess.STDOUT if capture else None),
    )

def sh(cmd):
    return subprocess.run(cmd, shell=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.strip()

# VPN helpers

def start_openvpn_daemon():
    pid_path = pathlib.Path(OVPN_PID)
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text().strip())
            if run(["ps", "-p", str(pid)], check=False).returncode == 0:
                print(f"OpenVPN ya está en marcha (pid {pid}).")
                return
        except Exception:
            pass
    # Seguridad: existe el .ovpn/.conf
    if not pathlib.Path(OVPN_CONF).exists():
        raise FileNotFoundError(f"No existe el perfil VPN: {OVPN_CONF}")

    print("Starting OpenVPN…")
    run([
        "sudo", "openvpn",
        "--config", OVPN_CONF,
        "--daemon",
        "--writepid", OVPN_PID
        ])

def tun_has_ip():
    out = sh(f"ip -o addr show dev {TUN_IFACE} | awk '/inet /{{print $4}}'")
    return bool(out.strip())

def wait_until_ready(timeout=WAIT_TIMEOUT):
    """
    Espera a que la VPN esté operativa:
    - Interfaz tun0 arriba y con IP
    """

    deadline = time.time() + timeout
    while time.time() < deadline:
        # interfaz existe
        if run(["ip", "addr", "show", "dev", TUN_IFACE], check=False).returncode == 0:
            if tun_has_ip():
                print("VPN connected")
                return
        time.sleep(1)

    raise TimeoutError(f"No se detectó la VPN en {timeout}s.")

def force_route_via_vpn(target_ip: str):
    """
    Obliga a que target_ip vaya por tun0.
    """
    if not tun_has_ip():
        raise RuntimeError("tun0 no tiene IP; no puedo forzar ruta.")

    # en interfaces TUN point-to-point, basta 'dev tun0'
    cmd = ["sudo", "ip", "route", "replace", f"{target_ip}/32", "dev", TUN_IFACE, "metric", "5"]
    run(cmd, check=False)
    #print("Route check:")
    #print(sh(f"ip route get {target_ip}"))

def stop_openvpn():
    pid_path = pathlib.Path(OVPN_PID)
    if not pid_path.exists():
        print("No hay pidfile; nada que parar.")
        return
    try:
        pid = pid_path.read_text().strip()
        if pid:
            print(f"Stopping OpenVPN…")
            run(["sudo", "kill", pid], check=False)
            # esperar a que termine
            for _ in range(20):
                if run(["ps", "-p", pid], check=False).returncode != 0:
                    break
                time.sleep(0.3)
    finally:
        try:
            pid_path.unlink(missing_ok=True)
        except Exception:
            pass

# SSH / DB helpers 

def ssh_connect():
    # Valida clave
    if not os.path.isfile(SSH_KEY):
        raise FileNotFoundError(f"Clave SSH no encontrada: {SSH_KEY}")
    os.chmod(SSH_KEY, 0o600)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        SSH_HOST,
        port=SSH_PORT,
        username=SSH_USER,
        key_filename=SSH_KEY,
        timeout=10,
        banner_timeout=10,
        auth_timeout=10,
        allow_agent=False,
        look_for_keys=False
    )
    return ssh

def ssh_disconnect(ssh):
    try:
        ssh.close()
    except Exception:
        pass

def build_query(entry_ids: list[str]) -> str:
    """
    Genera la query SQL con todos los entry_id que se pasen en la lista.
    """
    # Escapar cada id con comillas simples
    ids_str = ", ".join(f"'{eid}'" for eid in entry_ids)

    query = f"""
    SELECT
        p.entry_id,
        p.product_id,
        mc.mChannelName AS media_channel,
        mp.mPanelName AS audience,
        d.bucket_name,
        d.document_path,
        d.document_filename,
        co.companyName,
        (
            SELECT GROUP_CONCAT(DISTINCT s.sectorName ORDER BY s.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm
            JOIN csv2_master_lookup.cscan_sector s
                ON s.sectorID = psm.sector_id
            WHERE psm.product_id = p.product_id
            AND s.parentID = 0
        ) AS sectors
    FROM csv2_product.cscan_document d
    JOIN csv2_product.cscan_product p ON p.product_id = d.productID
    JOIN csv2_product.cscan_product_company_mapping cmap ON cmap.product_id = p.product_id
    JOIN csv2_master_lookup.cscan_company co ON co.companyID = cmap.company_id
    JOIN csv2_master_lookup.cscan_mchannel mc ON mc.mChannelID = p.mchanne_id
    JOIN csv2_master_lookup.cscan_mpanel mp ON mp.mPanelID = p.mpanel_id

    WHERE p.entry_id IN ({ids_str}) AND cmap.default_img=1;
    """
    return query.strip()

def build_query_imgs(entry_ids: list[str]) -> str:
    """
    Genera la query SQL con todos los entry_id que se pasen en la lista.
    """
    # Escapar cada id con comillas simples
    ids_str = ", ".join(f"'{eid}'" for eid in entry_ids)

    query = f"""
    SELECT 
        p.entry_id,
        p.product_id,
        i.img_document_filename
    FROM csv2_product.cscan_document d
    JOIN csv2_product.cscan_product p 
        ON p.product_id = d.productID
    JOIN csv2_product.cscan_img_document i 
        ON i.productID = p.product_id
    WHERE p.entry_id IN ({ids_str})
      AND i.img_document_default = 1;
    """
    return query.strip()

def build_query_by_company(company_name: str, limit: int = 10, media_channel: str = None) -> str:
    """
    Returns the most recent entries for a given company name with full metadata.
    Optionally filter by media channel (e.g. 'Direct Mail', 'Email').
    """
    channel_filter = f"AND mc.mChannelName = '{media_channel}'" if media_channel else ""
 
    query = f"""
    SELECT
        p.entry_id,
        p.product_id,
        mc.mChannelName AS media_channel,
        mp.mPanelName AS audience,
        d.bucket_name,
        d.document_path,
        d.document_filename,
        co.companyName,
        (
            SELECT GROUP_CONCAT(DISTINCT s.sectorName ORDER BY s.sectorName SEPARATOR '||')
            FROM csv2_product.cscan_product_sector_mapping psm
            JOIN csv2_master_lookup.cscan_sector s
                ON s.sectorID = psm.sector_id
            WHERE psm.product_id = p.product_id
            AND s.parentID = 0
        ) AS sectors
    FROM csv2_product.cscan_document d
    JOIN csv2_product.cscan_product p ON p.product_id = d.productID
    JOIN csv2_product.cscan_product_company_mapping cmap ON cmap.product_id = p.product_id
    JOIN csv2_master_lookup.cscan_company co ON co.companyID = cmap.company_id
    JOIN csv2_master_lookup.cscan_mchannel mc ON mc.mChannelID = p.mchanne_id
    JOIN csv2_master_lookup.cscan_mpanel mp ON mp.mPanelID = p.mpanel_id
    WHERE co.companyName = '{company_name}'
    AND cmap.default_img = 1
    {channel_filter}
    ORDER BY p.entry_id DESC
    LIMIT {limit};
    """
    return query.strip()





def execute_command(ssh, mysql_host, mysql_user, mysql_passwd, query, expected_entry_ids, run_id):
    try:
        command = f"mysql -h {mysql_host} -u {mysql_user} -p{mysql_passwd} -e \"{query}\""
        _, stdout, _ = ssh.exec_command(command)
        raw_output = stdout.read().decode('utf-8')
        if not raw_output.strip():
            return False, None

        df = pd.read_csv(StringIO(raw_output), sep="\t")
        if df.empty or "media_channel" not in df.columns or "entry_id" not in df.columns:
            return False, None
        media_channel = df["media_channel"].iloc[0]
        csv_path = Path(BASE_DIR) / f"product_locations_{run_id}.csv"
        df.to_csv(csv_path, index=False)
        print("--------------------------------------------------")
        print(f"CSV with {len(df)} PDF locations created successfully.")
        print("--------------------------------------------------")

        if expected_entry_ids:
            found_ids = set(df['entry_id'].astype(str))
            expected_ids = set(str(e) for e in expected_entry_ids)

            missing = expected_ids - found_ids
            if missing:
                print("Missing entry_ids in DB result:", ", ".join(sorted(missing)))
                return False, None

        return True, media_channel
    except Error as err:
        print(err)
        return False, None


def execute_command_imgs(ssh, mysql_host, mysql_user, mysql_passwd, query, expected_entry_ids, run_id):
    try:
        command = f"mysql -h {mysql_host} -u {mysql_user} -p{mysql_passwd} -e \"{query}\""
        _, stdout, _ = ssh.exec_command(command)
        raw_output = stdout.read().decode('utf-8')
        df = pd.read_csv(StringIO(raw_output), sep="\t")
        csv_path = Path(BASE_DIR) / f"product_imgs_{run_id}.csv"
        df.to_csv(csv_path, index=False)

    except Error as err:
        print(err)


def update_query(database, target):
    with open('get_queries.sql', 'r') as file: sql_query = file.read()
    sql_query_updated = sql_query.replace('DATABASE', database)
    sql_query_updated = sql_query_updated.replace('TARGET', target)
    return sql_query_updated


# MAIN

def downloadLocations(list_entry_ids, run_id):
    try:
        # 1) Sube la VPN y espera
        #start_openvpn_daemon()
        #wait_until_ready()

        # 2) Fuerza que el destino SSH vaya por la VPN, sin este paso no funciona
        #force_route_via_vpn(SSH_HOST)

        # 3) SSH y consulta
        ssh = ssh_connect()
        try:
            query = build_query(list_entry_ids)
            ok, media_channel=execute_command(ssh, mysql_host, mysql_user, mysql_passwd, query,list_entry_ids, run_id)
            if media_channel != "Online Video":
                queryImgs = build_query_imgs(list_entry_ids)
                execute_command_imgs(ssh, mysql_host, mysql_user, mysql_passwd, queryImgs ,list_entry_ids, run_id)

        finally:
            ssh_disconnect(ssh)
        return ok

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    #finally:
    #    stop_openvpn()
    #    print("VPN stopped.")
