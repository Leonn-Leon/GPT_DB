from sentence_transformers import SentenceTransformer
from pathlib import Path
import yaml
import sqlite3
import sqlite_vec

db = sqlite3.connect(":memory:")
db.enable_load_extension(True)
sqlite_vec.load(db)
db.enable_load_extension(False)

model = SentenceTransformer('cointegrated/rubert-tiny2')
path_to_yaml = Path(__file__).parent / 'data' / 'confs' / 'otgruzki_structure.yaml'

def search_by_structure_of_table(strings) -> dict[str, list[tuple[str, str, float]]]:
    """
    Example of return:
    {
    'менеджер за сделку': [('VBRK_ZZPERNR_ZM', 'Код. Менеджер, ответственный за сделку', 0.10274982452392578), ...]
    'Маржа ССЦ за позицию': [('ZAMARGPRF_RUB', 'Маржинальная прибыль от ССЦ за позицию в рублях.', 0.22930245101451874), ...]
    }    
    """

    with open(path_to_yaml, 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
        fields = [(model.encode(d['description']), d['description'], d['technical_name']) for d in data['table']['fields']]

    answer = {}
    for string in strings:
        answer[string] = []
        embedding_input_string = model.encode(string)
        for field in fields:
            cos_sim = db.execute('select vec_distance_cosine(?, ?)', [embedding_input_string, field[0]]).fetchone()[0]
            to_answer = (field[2], field[1], cos_sim)
            answer[string].append(to_answer)

        answer[string].sort(key=lambda x: x[2])
    return answer


if __name__ == "__main__":
    test_query = ['на урале']
    print(search_by_structure_of_table(test_query))
