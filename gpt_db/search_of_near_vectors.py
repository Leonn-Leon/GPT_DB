import sqlite3
import sqlite_vec
from sentence_transformers import SentenceTransformer
from pathlib import Path

model = SentenceTransformer('cointegrated/rubert-tiny2')
path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'

def search_of_near_vectors(strings) -> dict[str, list[tuple[str, str, str, str]]]:
    """
    Example of return:
    {
    'Арматура десятка А400': [('ZPRODH31', 0.06224990263581276, 'Арматура 10 А400', '100100140001900'), ('ZPRODH21', 0.13169412314891815, 'Каркас треуг 8 А500С', '3054004700'), ]
    'филиал екатеринбург': [('ZCFO1', 0.16964463889598846, 'СПК-Екатеринбург', '0802'), ('ZCUSTOMER', 0.18706147372722626, 'ООО Ветрикаль', '9000719800'), ]
    }    
    """
    connection = sqlite3.connect(path_to_db)
    connection.enable_load_extension(True)
    sqlite_vec.load(connection)
    connection.enable_load_extension(False)

    names_of_tables_query = connection.execute("select name from sqlite_master where type='table'")
    names_of_tables = [table[0] for table in names_of_tables_query.fetchall() if table[0] != 'ZARM_AUTH_CFO']

    answer = {}
    for string in strings:
        answer[string] = []
        embedding = model.encode(string)
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
            ).fetchone()
        
            to_answer = (name_of_table,) + row
            answer[string].append(to_answer)

        answer[string].sort(key=lambda x: x[1])

    connection.close()
    return answer


if __name__ == "__main__":
    test_query = ['Арматура десятка А400', 'филиал екатеринбург']
    print(search_of_near_vectors(test_query))