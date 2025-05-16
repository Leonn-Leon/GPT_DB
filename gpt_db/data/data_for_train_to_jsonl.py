import json
from data_for_train import examples
# преобразуем data_for_train_1.py в jsonl. data_for_train_1.py оставил, тк его легче читать/править

with open('confs/otgruzki_structure.txt', 'r', encoding='utf-8') as file:
    struct_of_zzsdm_117_cust = file.read()

#with open('confs/divisions.txt', 'r', encoding='utf-8') as file:
#    textes_of_zdiv = file.read()


with open('data_for_train.jsonl', 'w', encoding='utf-8') as f:
    for ex in examples:
        new_line = {"text": f"Ты эксперт в SQL и аналитике данных.\n Тебе дана структура таблицы ZSDM_117_CUST: {struct_of_zzsdm_117_cust}\nВопрос: {ex['question']} ", 
                    "label": ex['answer']}
        new_line_json = json.dumps(new_line, ensure_ascii=False)
        f.write(new_line_json + '\n')

