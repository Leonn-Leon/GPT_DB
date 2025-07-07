import sqlite3
from sqlglot import parse_one, exp, condition
from pathlib import Path

path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'

def apply_restrictions(sql_query, user):
    try:
        parsed = parse_one(sql_query)
    except Exception:
        return sql_query, False
    connection = sqlite3.connect(path_to_db)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM ZARM_AUTH_CFO where zuser = ?', (user,))
    rows = cursor.fetchall()
    connection.close()
    
    #если нет прав добавляем невыполнимый фильтр (строка не найдена). если auth - пустая строка, ограничений нет
    if not rows:
        auth = '1 = 2'
    elif rows[0]['auth'] == '':
        return parsed.sql(pretty=True, identify=True), True
    else:
        auth = rows[0]['auth']

    where_condition = condition(auth)
    parsed_with_restriction = parse_one(sql_query).where(where_condition)
    result_sql = parsed_with_restriction.sql(pretty=True, identify=True)
    
    return result_sql, True


if __name__ == "__main__":
    test_query = """
SELECT SUM(ZQSHIPTOF) AS TotalShipmentInTons
FROM SAPABAP1.ZZSDM_117_CUS
WHERE ZDIV = '02' AND substr(VBRK_FKDAT, 1, 6) = '202502'
GROUP BY DATE;
"""
    test = apply_restrictions(test_query, 'user1')
    print(test)