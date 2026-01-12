from typing import Optional
import mysql.connector

# DBのコネクションを返す
def createConnection(host):
    try:
        conn = mysql.connector.connect(
            host='localhost',       # mysqlのサーバーアドレス
            user='admin',            # mysqlのユーザーID
            password='password',    # mysqlのrootユーザーのパスワード
            port=3306,              # mysqlのポート番号
            database='offchaindb'
        )
    except mysql.connector.Error as e:
        print(f"error:{e}")
        return None
    return conn
    
if __name__ == '__main__':

    hosts = ['A']

    for host in hosts:

        connection_set = {'A', 'B', 'C'} - {host}

        conn = cur =None

        try:
            conn = createConnection(host)
            cur = conn.cursor()

            try:
                cur.execute(f'DROP INDEX index_cfpval ON {host}_cfpval')
            except mysql.connector.Error:
                pass
            
            try:
                cur.execute(f'DROP INDEX index_parts_tree ON {host}_parts_tree')
            except mysql.connector.Error:
                pass

            try:
                cur.execute(f'DROP INDEX index_assembler ON {host}_assembler')
            except mysql.connector.Error:
                pass

            # テーブル作成
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {host}_cfpval(
                    partid VARCHAR(50) PRIMARY KEY,
                    cfp DECIMAL(18,4) NOT NULL,
                    co2 DECIMAL(18,4) NOT NULL
                )
            ''')
            cur.execute(f'CREATE INDEX index_cfpval ON {host}_cfpval (partid)')

            # parts_tree
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {host}_parts_tree (
                    partid VARCHAR(50),
                    parents_partid VARCHAR(50),
                    qty BIGINT,
                    UNIQUE (partid, parents_partid)
                )
            ''')
            cur.execute(f'CREATE INDEX index_parts_tree ON {host}_parts_tree (partid)')

            # assembler
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS {host}_assembler (
                    partid VARCHAR(50) PRIMARY KEY,
                    assembler VARCHAR(50) NOT NULL
                )
            ''')
            cur.execute(f'CREATE INDEX index_assembler ON {host}_assembler (partid)')
        
            conn.commit()

        except mysql.connector.Error as e:
            print(f"error:{e}")
        finally:
            cur.close()
            conn.close()