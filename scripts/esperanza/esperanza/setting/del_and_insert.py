# datasetをpostgresに挿入するスクリプト

from typing import Optional
import psycopg
import mysql.connector
import sys 

#hosts = ['A', 'B', 'C']
hosts = ['A']

# DBのコネクションを返す
def createConnection_p(host):
    try:
        conn = psycopg.connect(
            dbname='iroha_default',
            user='postgres',
            password='mysecretpassword',
            port='5432',
            host='postgres'+host
        )
    except psycopg.Error as e:
        print(f"error:{e}")
        return None
    
    return conn

# DBのコネクションを返す
def createConnection_m(host):
    try:
        conn = mysql.connector.connect(
            host='localhost',       # mysqlのサーバーアドレス
            user='admin',            # mysqlのユーザーID
            password='password',    # mysqlのrootユーザーのパスワード
            port=3306,              # mysqlのポート番号
            database='offchaindb',
            allow_local_infile=True
        )
    except mysql.connector.Error as e:
        print(f"error:{e}")
        return None
    
    return conn




def execDelete_m():

    for host in hosts:

        conn = cur =None

        try:
            conn = createConnection_m(host)
            cur = conn.cursor()

            cur.execute(f"delete from {host}_assembler;")
            cur.execute(f"delete from {host}_parts_tree;")
            cur.execute(f"delete from {host}_cfpval;")
            conn.commit()

        except mysql.connector.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()



def execDelete_p():

    sql = """
        delete from hash_parts_tree;
    """

    conn: Optional[psycopg.Connection] = None
    
    for host in hosts:

        try:
            conn = createConnection_p(host)
            cur = conn.cursor()

            cur.execute(sql)
            conn.commit()

        except psycopg.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()





def execInsert_m(parameter):
    
    tables = ['assembler', 'parts_tree', 'cfpval']

    for host in hosts:

        conn = cur =None
        
        filename = [f'assembler{host}.csv', f'parts_tree{host}.csv', f'cfpval{host}.csv']

        try:
            conn = createConnection_m(host)
            cur = conn.cursor()

            for i in range(3):

                cwd = os.getcwd()
                #infile = f"/root/framework_APP/setting/dataset/{parameter}/{filename[i]}"
                infile = f"dataset/{filename[i]}"

                sql = f"""
                    LOAD DATA LOCAL INFILE '{infile}'
                    INTO TABLE {host}_{tables[i]}
                    FIELDS TERMINATED BY ','
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\\r\\n'
                    IGNORE 1 ROWS;
                """

                cur.execute(sql)

            conn.commit()

        except mysql.connector.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()


def execInsert_p(parameter):
    
    conn: Optional[psycopg.Connection] = None

    for host in hosts:

        try:
            conn = createConnection_p(host)
            cur = conn.cursor()

            infile = f"dataset/{parameter}/hash_parts_tree.csv"

            copy_sql = f"""
                COPY hash_parts_tree FROM STDIN WITH (FORMAT csv, HEADER true)
            """

            with cur.copy(copy_sql) as copy:
                with open(infile, "rb") as f:
                    copy.write(f.read())

            conn.commit()

        except psycopg.Error as e:
            print(f"error: {e}")

        finally:
            cur.close()
            conn.close()




    
if __name__ == '__main__':

    execDelete_m()
    #execDelete_p()

    args = sys.argv

    #parameter = args[1] if len(args) > 1 else '0/30000/3' 
    parameter = '0/30000/0'

    execInsert_m(parameter)
    #execInsert_p(parameter)
    