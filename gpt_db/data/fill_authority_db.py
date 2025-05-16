import pandas as pd
import os

# Имя CSV файла
CSV_FILE = 'gpt_db/data/authority.csv'
COLUMNS = ['zuser', 'auth']

# Данные для добавления (такие же, как у вас)
data_to_add_list = [
    ('user5', "(ZCFO1 in ('4505','1002','1102','1303','1403','1502','2402',) or (ZDIV = '04'))"),
    ('user4', "(ZDIV = '03')"),
    ('user3', "(ZDIV = '01')"),
    ('user2', "(ZDIV = '01')"),
    ('user0', ""),
]

# Преобразуем новые данные в DataFrame
new_data_df = pd.DataFrame(data_to_add_list, columns=COLUMNS)

# Проверяем, существует ли CSV-файл
if os.path.exists(CSV_FILE):
    # Загружаем существующие данные
    existing_df = pd.read_csv(CSV_FILE)
    # Проверяем, что колонки совпадают, на случай если файл был изменен вручную
    if not all(col in existing_df.columns for col in COLUMNS) or len(existing_df.columns) != len(COLUMNS):
        print(f"Предупреждение: Колонки в {CSV_FILE} не соответствуют ожидаемой схеме. Файл будет перезаписан новыми данными.")
        df_to_save = new_data_df
    else:
            # Убедимся, что типы данных согласованы (особенно важно для пустых или ранее созданных файлов)
        for col in COLUMNS:
            if col not in existing_df:
                existing_df[col] = pd.Series(dtype='object') # или подходящий тип
        # Приводим к строковому типу, чтобы избежать проблем со смешанными типами при конкатенации
        existing_df = existing_df.astype({col: str for col in COLUMNS})
        new_data_df = new_data_df.astype({col: str for col in COLUMNS})

        # Добавляем новые данные к существующим
        df_to_save = pd.concat([existing_df, new_data_df], ignore_index=True)
else:
    # Если файл не существует, используем только новые данные
    print(f"Файл {CSV_FILE} не найден. Будет создан новый.")
    df_to_save = new_data_df

df_to_save.to_csv(CSV_FILE, index=False, encoding='utf-8')

print(f"\nДанные успешно записаны в {CSV_FILE}")

# Выведем содержимое файла для проверки
print("\nСодержимое файла:")
print(pd.read_csv(CSV_FILE))