from sentence_transformers import SentenceTransformer
import sqlite3
import sqlite_vec
from typing import List
from sqlite_vec import serialize_float32
import csv
from pathlib import Path

model = SentenceTransformer('cointegrated/rubert-tiny2') 

path_to_csv_foled = Path(__file__).parent / "confs" / "csv"
csv_files = [f.stem for f in path_to_csv_foled.glob("*.csv")] 

path_to_db = Path(__file__).parent / 'sqlite.db'
connection = sqlite3.connect(path_to_db)
cursor = connection.cursor()

connection.enable_load_extension(True)
sqlite_vec.load(connection)
connection.enable_load_extension(False)

for name in csv_files:
	path_to_csv = Path(__file__).parent / "confs" / "csv" / f"{name}.csv"
	with open(path_to_csv, 'r', encoding='utf-8') as file:
		reader = csv.reader(file, delimiter=';')
		next(reader) #skip head
		result = [(row[0], row[1], serialize_float32(model.encode(row[1]))) for row in reader]

	cursor.execute(f'''
	CREATE TABLE IF NOT EXISTS {name} (
	KEY TEXT PRIMARY KEY,
	TXT TEXT,
	VECTOR BLOB
	)
	''')

	insert_query_for_zarm_auth_cfo = f'INSERT INTO {name} (KEY, TXT, VECTOR) VALUES (?, ?, ?);'
	cursor.executemany(insert_query_for_zarm_auth_cfo, result)

connection.commit()
connection.close()