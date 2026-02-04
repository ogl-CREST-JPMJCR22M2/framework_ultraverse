from __future__ import annotations

# お手本
def get_review_item_by_id(conn, cursor, i_id: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "SELECT * FROM review r, item2 i WHERE i.i_id = r.i_id and r.i_id=%s "
        "ORDER BY rating DESC, r.creation_date DESC LIMIT 10",
        (i_id,),
    )
    cursor.fetchall()
    cursor.execute("COMMIT")

# 以下使用
def A_update_cfp(conn, cursor, partid: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE A_cfpval SET cfp = cfp - 0.001 WHERE partid=%s",
        (partid,),
    )
    cursor.execute("COMMIT")


def B_update_cfp(conn, cursor, partid: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE B_cfpval SET cfp = cfp - 0.001 WHERE partid=%s",
        (partid,),
    )
    cursor.execute("COMMIT")

def C_update_cfp(conn, cursor, partid: int) -> None:
    cursor.execute("START TRANSACTION")
    cursor.execute(
        "UPDATE C_cfpval SET cfp = cfp - 0.001 WHERE partid=%s",
        (partid,),
    )
    cursor.execute("COMMIT")


