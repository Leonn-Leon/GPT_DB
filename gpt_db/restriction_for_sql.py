import sqlite3
from sqlglot import parse_one, exp, condition

def apply_restrictions(sql_query, user, report):
    zvobj = ''
    auth = ''

    connection = sqlite3.connect('data/authority.db')
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM ZARM_AUTH_CFO where zuser = ? and (zvobj = ? or zvobj in ("7*", "*"))', (user, report,))
    rows = cursor.fetchall()
    connection.close()

    #находим строку c фильтром (находим строку с самой длинной длинной, тоесть проверяем 7.117, затем 7*, затем *)
    for row in rows:
        if len(row['zvobj']) > len(zvobj):
            zvobj = row['zvobj']
            auth = row['auth']

    #если нет прав добавляем невыполнимый фильтр
    if not auth:
        auth = '1 = 2'
    #print(auth)

    parsed = parse_one(sql_query)
    where_condition = condition(auth)
    parsed_with_restriction = parsed.where(where_condition)
    result_sql = parsed_with_restriction.sql(pretty=True, identify=True)
    
    return result_sql


if __name__ == "__main__":
    test_query = """
SELECT SUM(ZQSHIPTOF) AS TotalShipmentInTons
FROM SAPABAP1.ZZSDM_117_CUS
WHERE ZDIV = '02' AND substr(VBRK_FKDAT, 1, 6) = '202502'
GROUP BY DATE;
"""
    apply_restrictions(test_query, 'user1', '7.117')