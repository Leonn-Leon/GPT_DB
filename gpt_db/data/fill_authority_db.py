import sqlite3
from pathlib import Path

path_to_db = Path(__file__).parent / 'sqlite.db'
connection = sqlite3.connect(path_to_db)
cursor = connection.cursor()

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
insert_query_for_zarm_auth_cfo = """INSERT OR REPLACE INTO ZARM_AUTH_CFO (zuser, auth) VALUES (?, ?);"""
cursor.executemany(insert_query_for_zarm_auth_cfo, data_for_zarm_auth_cfo)


connection.commit()
connection.close()