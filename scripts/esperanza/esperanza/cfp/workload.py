from __future__ import annotations

import random
import sys
import mysql.connector

from .constants import TRANSACTION_WEIGHTS
from .random_utils import random_string
from .transactions import (
    A_update_cfp,
    B_update_cfp,
    C_update_cfp,
)

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional dependency for progress UI
    tqdm = None


class CFPWorkloadExecutor:
    def __init__(self, conn, query_count: int) -> None:
        self.conn = conn
        self.query_count = query_count
        self.rng = random.Random()

    def run(self) -> dict:
        stats = {tx_name: 0 for tx_name in TRANSACTION_WEIGHTS}
        progress = self._new_progress(self.query_count, "workload")

        self.cached_rows = {}
        for host in ['A', 'B', 'C']:
            config = {
                    'user': 'deploy_user',
                    'password': 'password',
                    'host': 'ubuntu' + host,
                    'database': 'offchaindb',
                    'raise_on_warnings': True
            }
            conn = mysql.connector.connect(**config)
            try:
                conn.autocommit = False
                cursor = conn.cursor()
                cursor.execute(f"SELECT partid FROM {host}_cfpval LIMIT 100")
                results = cursor.fetchall()
                
                self.cached_rows[host] = results

                print(f"Host {host} loaded: {results[0]}")
            finally:
                if conn.is_connected():
                    cursor.close()
                    conn.close()

        try:
            for _ in range(self.query_count):
                tx_type = random.choices(
                    list(TRANSACTION_WEIGHTS.keys()),
                    weights=list(TRANSACTION_WEIGHTS.values()),
                    k=1,
                )[0]
                cursor = self.conn.cursor()
                try:
                    if tx_type == "A_update_cfp":
                        partid = self.cached_rows['A'][self.rng.randint(0, 99)][0]
                        A_update_cfp(self.conn, cursor, partid)
                    elif tx_type == "B_update_cfp":
                        partid = self.cached_rows['B'][self.rng.randint(0, 99)][0]
                        B_update_cfp(self.conn, cursor, partid)
                    elif tx_type == "C_update_cfp":
                        partid = self.cached_rows['C'][self.rng.randint(0, 99)][0]
                        C_update_cfp(self.conn, cursor, partid)
                    else:
                        raise ValueError(f"unknown transaction type: {tx_type}")

                    stats[tx_type] += 1
                finally:
                    cursor.close()

                if progress is not None:
                    progress.update(1)
        finally:
            if progress is not None:
                progress.close()

        return stats

    def _new_progress(self, total: int, desc: str):
        if tqdm is None or not sys.stderr.isatty():
            return None
        return tqdm(
            total=total,
            desc=desc,
            unit="tx",
            ascii=True,
            dynamic_ncols=True,
            mininterval=0.5,
        )
