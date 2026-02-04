import os
import subprocess
import shutil
import glob
from pathlib import Path
from datetime import datetime

REMOTE_USER = "root"
REMOTE_DIR = "/var/lib/mysql"
REMOTE_FILE_PATTERN = "myserver-binlog.*"  

LOCAL_MYSQL_DIR = Path("/var/lib/mysql")
LOCAL_INDEX_FILE = "myserver-binlog.index"
TEMP_DIR = Path("/tmp/imported_binlogs")

MYSQL_USER = "mysql"
MYSQL_GROUP = "mysql"
# ==============================================

def run_command(cmd, shell=True):
    try:
        subprocess.run(cmd, shell=shell, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {cmd}")
        raise e

def main(REMOTE_HOST):
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True)

    try:
        scp_cmd = f"scp {REMOTE_USER}@ubuntu{REMOTE_HOST}:{REMOTE_DIR}/{REMOTE_FILE_PATTERN} {TEMP_DIR}/"
        print(f"Executing: {scp_cmd}")
        run_command(scp_cmd)
    except Exception as e:
        print("SCP failed. Please check SSH connection.")
        return
    
    added_files = []

    for file_path in TEMP_DIR.glob("*"):
        filename = file_path.name
        
        if filename.endswith(".index"): # indexファイルをスキップ
            continue

        new_filename = f"myserver-binlog.{REMOTE_HOST}{len(added_files):05d}"
        dest_path = LOCAL_MYSQL_DIR / new_filename
        
        shutil.move(str(file_path), str(dest_path))
        
        shutil.chown(str(dest_path), user=MYSQL_USER, group=MYSQL_GROUP)
        
        added_files.append(new_filename)

    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)

    if not added_files:
        print("No binlog files were imported.")
        return
    
    # indexファイルの更新
    index_path = LOCAL_MYSQL_DIR / LOCAL_INDEX_FILE
    
    if not index_path.exists():
        index_path.touch()
        shutil.chown(str(index_path), user=MYSQL_USER, group=MYSQL_GROUP)

    with open(index_path, "a") as f:
        for fname in sorted(added_files):
            line = f"./{fname}\n"
            f.write(line)
            print(f"Appended to index: {line.strip()}")

if __name__ == "__main__":

    main("B")
    main("C")