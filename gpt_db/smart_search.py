import sqlite3
import sqlite_vec
from sentence_transformers import SentenceTransformer
from pathlib import Path
import yaml

class SmartSearch:
    def __init__(self):
        db = sqlite3.connect(":memory:")
        db.enable_load_extension(True)
        sqlite_vec.load(db)
        db.enable_load_extension(False)

        self.model = SentenceTransformer('cointegrated/rubert-tiny2')
        self.path_to_yaml = Path(__file__).parent / 'data' / 'confs' / 'otgruzki_structure.yaml'
        self.path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'
        self.db = db

    def search_of_near_vectors(self, strings) -> dict[str, list[tuple[str, float, str, str]]]:
        """
        Example of return:
        {
        'Арматура десятка А400': [('ZPRODH31', 0.06224990263581276, 'Арматура 10 А400', '100100140001900'), ('ZPRODH21', 0.13169412314891815, 'Каркас треуг 8 А500С', '3054004700'), ...]
        'филиал екатеринбург': [('ZCFO1', 0.16964463889598846, 'СПК-Екатеринбург', '0802'), ('ZCUSTOMER', 0.18706147372722626, 'ООО Ветрикаль', '9000719800'), ...]
        }    
        """
        connection = sqlite3.connect(self.path_to_db)
        connection.enable_load_extension(True)
        sqlite_vec.load(connection)
        connection.enable_load_extension(False)

        names_of_tables_query = connection.execute("select name from sqlite_master where type='table'")
        names_of_tables = [table[0] for table in names_of_tables_query.fetchall() if table[0] != 'ZARM_AUTH_CFO']

        answer = {}
        for string in strings:
            answer[string] = []
            embedding = self.model.encode(string)
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
    
    def search_by_structure_of_table(self, strings) -> dict[str, list[tuple[str, str, float]]]:
        """
        Example of return:
        {
        'менеджер за сделку': [('VBRK_ZZPERNR_ZM', 'Код. Менеджер, ответственный за сделку', 0.10274982452392578), ...]
        'Маржа ССЦ за позицию': [('ZAMARGPRF_RUB', 'Маржинальная прибыль от ССЦ за позицию в рублях.', 0.22930245101451874), ...]
        }    
        """

        with open(self.path_to_yaml, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
            fields = [(self.model.encode(d['description']), d['description'], d['technical_name']) for d in data['table']['fields']]

        answer = {}
        for string in strings:
            answer[string] = []
            embedding_input_string = self.model.encode(string)
            for field in fields:
                cos_sim = self.db.execute('select vec_distance_cosine(?, ?)', [embedding_input_string, field[0]]).fetchone()[0]
                to_answer = (field[2], field[1], cos_sim)
                answer[string].append(to_answer)

            answer[string].sort(key=lambda x: x[2])
        return answer

if __name__ == "__main__":
    import time

    test_query = ['урал', 'центральный дивизион', 'арматура']
    # Замерить время выполнения функции
    ss = SmartSearch()
    start_time = time.time()
    print(ss.search_of_near_vectors(test_query))
    print("--- %s seconds ---" % (time.time() - start_time))
    ####################
    
    # test_query = ['менеджер', 'маржа', "время/дата", 'клиент']
    # start_time = time.time()
    # print(ss.search_by_structure_of_table(test_query))
    # print("--- %s seconds ---" % (time.time() - start_time))