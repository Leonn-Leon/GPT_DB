import sqlite3

connection = sqlite3.connect('gpt_db/data/sqlite.db')
cursor = connection.cursor()

#connection.execute("DROP TABLE ZARM_AUTH_CFO") #ф-ия будет получать sql, пользователя и номер отчёта 7.....

cursor.execute('''
CREATE TABLE IF NOT EXISTS ZARM_AUTH_CFO (
zuser TEXT PRIMARY KEY,
auth TEXT
)
''')

data_for_zarm_auth_cfo = [
    ('user1', "(ZCFO1 in ('4505','1002','1102','1303','1403','1502','2402',) or (ZDIV = '04'))"),
    ('user2', "(ZDIV = '03')"),
    ('user3', "(ZDIV = '01')"),
    ('user4', "(ZDIV = '01')"),
    ('user0', ""),
]
insert_query_for_zarm_auth_cfo = """INSERT INTO ZARM_AUTH_CFO (zuser, auth) VALUES (?, ?);"""
cursor.executemany(insert_query_for_zarm_auth_cfo, data_for_zarm_auth_cfo)


connection.commit()
connection.close()