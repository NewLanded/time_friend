import psycopg2
from config import DB_NAME, HOST, PASSWORD, PORT, USER


def get_db_conn(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB_NAME):
    db_conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=db)
    return db_conn


def get_multi_data(db_conn, sql, args=None):
    cur = db_conn.cursor()
    try:
        if args is None:
            cur.execute(sql)
            result = cur.fetchall()
        else:
            cur.execute(sql, args)
            result = cur.fetchall()
    except Exception as e:
        raise e
    finally:
        cur.close()
    result = [list(i) for i in result]
    return result


def get_single_column(db_conn, sql, args=None):
    result = get_multi_data(db_conn, sql, args)
    result = [i[0] for i in result]
    return result


def get_single_row(db_conn, sql, args=None):
    cur = db_conn.cursor()
    try:
        if args is None:
            cur.execute(sql)
            result = cur.fetchone()
        else:
            cur.execute(sql, args)
            result = cur.fetchone()
    except Exception as e:
        raise e
    finally:
        cur.close()
    result = list(result)
    return result


def get_single_value(db_conn, sql, args=None):
    result = get_multi_data(db_conn, sql, args)
    result = None if not result else result[0][0]
    return result


def get_boolean_value(db_conn, sql, args=None):
    result = get_single_value(db_conn, sql, args)
    if result:
        return True
    else:
        return False


def update_data(db_conn, sql, args=None):
    cur = db_conn.cursor()
    try:
        if args is None:
            cur.execute(sql)
        else:
            cur.execute(sql, args)
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()
        raise e
    finally:
        cur.close()
