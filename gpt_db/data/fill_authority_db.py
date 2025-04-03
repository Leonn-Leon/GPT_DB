import sqlite3

connection = sqlite3.connect('authority.db')
cursor = connection.cursor()

#connection.execute("DROP TABLE ZARM_AUTH_CFO") #ф-ия будет получать sql, пользователя и номер отчёта 7.....

cursor.execute('''
CREATE TABLE IF NOT EXISTS ZARM_AUTH_CFO (
record INTEGER,
zuser TEXT,
zvobj TEXT,
auth TEXT,
PRIMARY KEY (record, zuser, zvobj)
)
''')

data_for_zarm_auth_cfo = [
    (1, 'user1', '7.117', "(ZCFO1 in ('4505','1002','1102','1303','1403','1502','2402',) or (ZDIV = '04'))"),
    (2, 'user1', '7*', "(ZDIV = '03')"),
    (2, 'user1', '*', "(ZDIV = '01')"),
    (2, 'user2', '7*', "(ZDIV = '01')"),
]
insert_query_for_zarm_auth_cfo = """INSERT INTO ZARM_AUTH_CFO (record, zuser, zvobj, auth) VALUES (?, ?, ?, ?);"""
cursor.executemany(insert_query_for_zarm_auth_cfo, data_for_zarm_auth_cfo)


connection.commit()
connection.close()