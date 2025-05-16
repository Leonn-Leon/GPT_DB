import csv
from data_for_train import examples
# преобразуем data_for_train_1.py в CSV. data_for_train_1.py оставил, тк его легче читать/править

keys = examples[0].keys()

with open('data_for_train.csv', 'w', encoding='utf-8', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(examples)


