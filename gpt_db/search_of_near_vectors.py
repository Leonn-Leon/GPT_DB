import sqlite3
import sqlite_vec
from pathlib import Path
import fasttext.util
import fasttext

fasttext.util.download_model('ru', if_exists='ignore')
model = fasttext.load_model('cc.ru.300.bin')
path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'

def search_of_near_vectors(strings: list[str]) -> dict[str, list[tuple[str, str]]]:    #dict[str, list[tuple[str, float, str, str]]]r: Пользователь str
    """Получает ключ и название справочника для каждой string из списка strings.
    Аргументы:
        strings: список строк list
    """
    if not strings: return []
    connection = sqlite3.connect(path_to_db)
    connection.enable_load_extension(True)
    sqlite_vec.load(connection)
    connection.enable_load_extension(False)

    names_of_tables_query = connection.execute("select name from sqlite_master where type='table'")
    names_of_tables = [table[0] for table in names_of_tables_query.fetchall() if table[0] not in ('ZARM_AUTH_CFO', 'data_for_train')]

    answer = {}
    for string in strings:
        string = string.replace("'", "").replace('"', '')
        answer[string] = []
        embedding = model.get_sentence_vector(string)
        for name_of_table in names_of_tables:
            row = connection.execute(
                f"""
                SELECT                    
                    vec_distance_cosine(VECTOR, ?) as DISTANCE,                    
                    TXT,
                    KEY
                FROM {name_of_table}
                WHERE TXT != ''
                ORDER BY DISTANCE
                LIMIT 1
                """,
                (sqlite_vec.serialize_float32(embedding),)
            ).fetchone()
        
            to_answer = (name_of_table,) + row
            answer[string].append(to_answer)
        answer[string].sort(key=lambda x: x[1])
        answer[string] = (answer[string][0][0], answer[string][0][3]) #####new

    connection.close()
    return answer


if __name__ == "__main__":
    test_query = ['Арматура десятка А400', 'Корнаухов Дмитрий Андреевич']
    print(search_of_near_vectors(['Арматура десятка А400', 'Урал', '"Урал"']))
