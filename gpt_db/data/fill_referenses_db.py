from sentence_transformers import SentenceTransformer
import sqlite3
import sqlite_vec
from typing import List
from sqlite_vec import serialize_float32
import csv
from pathlib import Path

model = SentenceTransformer('cointegrated/rubert-tiny2') #model for embeddings

csv_files = [f.stem for f in Path("gpt_db/data/confs").glob("*.csv")] #search csv files in current field

connection = sqlite3.connect('gpt_db/data/sqlite.db')
cursor = connection.cursor()

connection.enable_load_extension(True)
sqlite_vec.load(connection)
connection.enable_load_extension(False)

for name in csv_files:
	with open(f'gpt_db/data/confs/{name}.csv', 'r', encoding='utf-8') as file:
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