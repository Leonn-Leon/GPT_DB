import sqlite3
import sqlite_vec
from sentence_transformers import SentenceTransformer
from pathlib import Path

model = SentenceTransformer('cointegrated/rubert-tiny2')
path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'

def search_of_near_vectors(string, *key_strings) -> list[tuple[str, float, str, str]]:
    """
    Example of return:
    [('ZPRODH31', 0.06224990263581276, 'Арматура 10 А400', '100100140001900'), ('ZPRODH21', 0.13169412314891815, 'Каркас треуг 8 А500С', '3054004700'), ...]
    """
    connection = sqlite3.connect(path_to_db)
    connection.enable_load_extension(True)
    sqlite_vec.load(connection)
    connection.enable_load_extension(False)

    names_of_tables_query = connection.execute("select name from sqlite_master where type='table'")
    names_of_tables = [table[0] for table in names_of_tables_query.fetchall() if table[0] != 'ZARM_AUTH_CFO']

    embedding = model.encode(string)

    answer = []
    for name_of_table in names_of_tables:
        row = connection.execute(
            f"""
            SELECT                    
                vec_distance_cosine(VECTOR, ?) as DISTANCE,                    
                TXT,
                KEY
            FROM {name_of_table}
            ORDER BY DISTANCE
            LIMIT 1
            """,
            (sqlite_vec.serialize_float32(embedding),)
        ).fetchall()

        #проверям содержание key_strings в TXT при наличии key_strings
        TXT = row[0][1]
        if key_strings and not all(key_string.upper() in TXT.upper() for key_string in key_strings):
            continue

        to_answer = (name_of_table,) + row[0]
        answer.append(to_answer)

    connection.close()

    answer.sort(key=lambda x: x[1])
    if answer: return answer


if __name__ == "__main__":
    res = search_of_near_vectors('Арматура десятка А400', 'Арматура', 'А400')
    res = search_of_near_vectors('Арматура десятка А400')
    print(res)