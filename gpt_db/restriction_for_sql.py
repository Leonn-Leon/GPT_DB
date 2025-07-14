import sqlite3
from sqlglot import parse_one, exp, condition
from pathlib import Path

path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'

def apply_restrictions(sql_query, user):
    parsed = parse_one(sql_query)

    connection = sqlite3.connect(path_to_db)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute('SELECT * FROM ZARM_AUTH_CFO where zuser = ?', (user,))
    row = cursor.fetchone()
    
    #если нет прав добавляем невыполнимый фильтр (строка не найдена). если auth - пустая строка, ограничений нет
    if not row:
        auth = '1 = 2'
    elif row['auth'] == '':
        return parsed.sql(pretty=True, identify=True), 'Полномочия: Без ограничений'
    else:
        auth = row['auth']
    where_condition = condition(auth)
    parsed_with_restriction = parsed.where(where_condition)
    result_sql = parsed_with_restriction.sql(pretty=True, identify=True) #финальный sql

    #генерация комментария по полномочиям
    if auth == '1 = 2':
        auth = 'Доступ запрещён'
    else:
        zcfo1_rows = cursor.execute('SELECT * FROM ZCFO1').fetchall()
        zcfo1_dict = {zcfo1_row['KEY'] : zcfo1_row['TXT_1'] for zcfo1_row in zcfo1_rows}
        zdiv_rows = cursor.execute('SELECT * FROM ZDIV').fetchall()
        zdiv_dict = {zdiv_row['KEY'] : zdiv_row['TXT_1'] for zdiv_row in zdiv_rows}
        connection.close()
        zcfo1_dict.update(zdiv_dict) #объединяем словари
        auth = auth.replace('ZCFO1 in', 'Филиалы из списка:').replace('ZDIV in', 'Дивизионы из списка:').replace('ZCFO1 =', 'Филиал =').replace('ZDIV =', 'Дивизион =').replace("'", "").replace("or", "или").replace(",", ", ")
        for key, val in zcfo1_dict.items():
            if key in auth:
                auth = auth.replace(key, val)
    auth = 'Полномочия: ' + auth
    
    return result_sql, auth


if __name__ == "__main__":
    test_query = """
SELECT SUM(ZQSHIPTOF) AS TotalShipmentInTons
FROM SAPABAP1.ZZSDM_117_CUS
WHERE ZDIV = '02' AND substr(VBRK_FKDAT, 1, 6) = '202502'
GROUP BY DATE;
"""
    test, auth = apply_restrictions(test_query, 'HODYKINVYU')
    print(test, '\n', auth)