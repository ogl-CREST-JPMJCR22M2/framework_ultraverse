### partidを使って求める

import time
import hashlib
from decimal import *
from typing import Optional
import psycopg
from psycopg import sql
import mysql.connector
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait

import commons as com

#==========#
# 必要設定  #
#==========#

assemblers = ["postgresA", "postgresB", "postgresC"]
ubuntus = ['ubuntuA', 'ubuntuB', 'ubuntuC']


#====================#
# ハッシュ部品木の生成  #
#====================#

def make_hash_parts_tree(assembler, root_partid):
    data = []
    conn_m = None
    cur_m = None

    try:
        # mysqlコネクション
        conn_m = com.createConnection_m(assembler)
        conn_m.autocommit = True
        cur_m = conn_m.cursor(buffered=True)

        # ==========================================
        # 1. 必要なデータをDBからget (Fetch)
        # ==========================================
        
        # ツリー構造と重複情報の取得
        sql_tree = f"""
            WITH RECURSIVE all_tree AS (
                SELECT partid, parents_partid, qty FROM A_parts_tree
                UNION ALL
                SELECT partid, parents_partid, qty FROM B_parts_tree
                UNION ALL
                SELECT partid, parents_partid, qty FROM C_parts_tree
            ),
            get_tree AS ( 
                SELECT partid, parents_partid, qty FROM all_tree WHERE partid = %s
                UNION DISTINCT
                SELECT a.partid, a.parents_partid, a.qty
                FROM all_tree a
                JOIN get_tree gt ON a.parents_partid = gt.partid 
            )
            SELECT gt.partid, gt.parents_partid, gt.qty 
            FROM get_tree gt;
        """
        cur_m.execute(sql_tree, (root_partid, ))
        tree_rows = cur_m.fetchall()

        # CO2の取得
        cur_m.execute("""
            SELECT partid, co2 FROM A_cfpval
            UNION ALL SELECT partid, co2 FROM B_cfpval
            UNION ALL SELECT partid, co2 FROM C_cfpval
        """)
        co2_map = {row[0]: float(row[1]) for row in cur_m.fetchall()}

        # Assemblerの取得 
        cur_m.execute("""
            SELECT partid, assembler FROM A_assembler
            UNION ALL SELECT partid, assembler FROM B_assembler
            UNION ALL SELECT partid, assembler FROM C_assembler
        """)
        assembler_map = {row[0]: row[1] for row in cur_m.fetchall()}


        # ==========================================
        # 2. Pythonメモリ上で計算 (Calculation)
        # ==========================================

        # データ構造の構築
        # DAGとして扱う
        nodes = {}
        for pid, parent, qty in tree_rows:
            if pid not in nodes: nodes[pid] = {'children': [], 'dup_count': 0}
            if parent and parent not in nodes: nodes[parent] = {'children': [], 'dup_count': 0}
            
            # 子情報を登録
            if parent:
                nodes[parent]['children'].append(pid)
            
            # 重複カウント
            nodes[pid]['dup_count'] += 1

        # --- CFP計算---
        cfp_cache = {}

        def get_cfp(current_partid):
            # 既に計算済みならキャッシュを返す
            if current_partid in cfp_cache:
                return cfp_cache[current_partid]
            
            my_co2_unit = co2_map.get(current_partid, 0.0)
            
            pass 
        
        # 一時テーブル作成 (CFP計算用)
        cur_m.execute("CREATE TABLE IF NOT EXISTS target_tree (partid VARCHAR(50), parents_partid VARCHAR(50), qty DECIMAL(65,0))")
        cur_m.execute("TRUNCATE TABLE target_tree") # 残存データ消去
        
        # executemanyで一括挿入
        if tree_rows:
            cur_m.executemany("INSERT INTO target_tree (partid, parents_partid, qty) VALUES (%s, %s, %s)", tree_rows)
        
        # CFP計算とBaseハッシュの取得 
        sql_calc_base = """
            WITH RECURSIVE 
            calc_qty(partid, root, quantity) AS (
                SELECT DISTINCT tt.partid, tt.partid AS root, CAST(1 AS DECIMAL(65,0))
                FROM target_tree tt
                UNION ALL
                SELECT tt.partid, cq.root, CAST((cq.quantity * tt.qty) AS DECIMAL(65,0))
                FROM calc_qty cq JOIN target_tree tt ON tt.parents_partid = cq.partid
            ),
            all_co2vals AS (
                SELECT partid, co2 FROM A_cfpval
                UNION ALL SELECT partid, co2 FROM B_cfpval
                UNION ALL SELECT partid, co2 FROM C_cfpval
            ),
            cfpvals AS (
                SELECT cq.root AS partid, ROUND(SUM(all_co2.co2 * cq.quantity), 4) AS cfp
                FROM calc_qty cq
                JOIN all_co2vals all_co2 ON cq.partid = all_co2.partid
                GROUP BY cq.root
            )
            SELECT partid, cfp, SHA2(CAST(cfp AS CHAR), 256) FROM cfpvals;
        """
        cur_m.execute(sql_calc_base)
        
        # Python辞書へロード
        part_data = {}
        for row in cur_m.fetchall():
            p_id, cfp_val, hash_hex = row
            
            part_data[p_id] = {
                'cfp': cfp_val,
                'base_hash': int(hash_hex, 16), 
                'final_hash': None
            }

        # 親子関係マップ作成 

        dup_check = {}
        children_map = {} 

        for child, parent, _ in tree_rows:
            dup_check[child] = dup_check.get(child, 0) + 1
            if parent:
                if parent not in children_map: children_map[parent] = []
                # ここではまだ重複フラグ確定できないので後で
                children_map[parent].append(child)

        # 重複フラグ付きでchildren_mapを整理
        final_children_map = {}
        for p, children in children_map.items():
            final_children_map[p] = []
            for c in children:
                is_dup = (dup_check.get(c, 0) > 1)
                final_children_map[p].append((c, is_dup))

        # --- ハッシュ再帰計算 (XOR) ---
        
        def get_final_hash(p_id):
            if p_id not in part_data:
                return 0 # エラーガード
            
            # メモ化
            if part_data[p_id]['final_hash'] is not None:
                return part_data[p_id]['final_hash']

            # 計算: Base Hash (CFP由来)
            current_val = part_data[p_id]['base_hash']

            # 子供のハッシュをXOR
            if p_id in final_children_map:
                for child_id, is_dup in final_children_map[p_id]:
                    child_hash_val = get_final_hash(child_id)
                    
                    val_to_xor = child_hash_val
                    if is_dup:
                        
                        child_bytes = child_hash_val.to_bytes(32, 'big')
                        
                        # 親IDの結合 (UTF-8エンコード)
                        combined = child_bytes + p_id.encode('utf-8')
                        
                        # ハッシュ化して整数に戻す
                        hashed = hashlib.sha256(combined).digest()
                        val_to_xor = int.from_bytes(hashed, 'big')
                    
                    current_val ^= val_to_xor

            part_data[p_id]['final_hash'] = current_val
            return current_val

        # 全ノード計算実行
        final_result_list = []
        for pid in part_data:
            h_val = get_final_hash(pid)
            
            # 戻り値データの作成
            # format: (partid, assembler, cfp, hashval(hex))
            assembler_name = assembler_map.get(pid, None)
            cfp_val = part_data[pid]['cfp']
            hash_hex = f"{h_val:064x}" # 64桁のHex文字列に
            
            # assembler情報があるものだけ返す（元のSQLの挙動）
            if assembler_name:
                final_result_list.append((pid, assembler_name, cfp_val, hash_hex))

        data = final_result_list

    except Exception as e:
        print(f"Error: {e}")
        # 必要なら再送出
        # raise e
    finally:
        if conn_m and conn_m.is_connected():
            cur_m.execute("DROP TABLE IF EXISTS target_tree")
            cur_m.close()
            conn_m.close()
    
    return data


def tree_generation_process(assembler, root_partid):

    # ハッシュ部品木生成

    start_time = time.time() # timer

    result = make_hash_parts_tree(assembler, root_partid)

    print("Tree generation time:", time.time() - start_time) # timer
    
    if not result:
        print("No data found.")
        return

    part_ids, assemblers_list, cfps, hashes = zip(*result)

    # DB書き込み用辞書の作成
    # zipで回すことでインデックスアクセスを回避
    insert_val_dict = defaultdict(list)
    for asm, cfp, pid in zip(assemblers_list, cfps, part_ids):
        insert_val_dict[asm].append((cfp, pid))

    # irohaコマンド
    #start_time = time.time() # timer
    #com.IROHA_CMDexe(assembler, list(part_ids), list(hashes))
    #print("iroha time:", time.time() - start_time) # timer

    # offchain-DBへの並列write
    with ThreadPoolExecutor() as executor:
        futures = []
        for asm_key, data_list in insert_val_dict.items():
            futures.append(executor.submit(com.update_db_worker, asm_key, data_list))
        
        # 全スレッドの完了を待機
        wait(futures)


# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'
    assembler = 'A'

    start = time.time()

    #result = make_hash_parts_tree(assembler, root_partid)
    #print(result[0])

    tree_generation_process(assembler, root_partid)
    
    t = time.time() - start
    print("time:", t)