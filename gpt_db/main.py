import os
import json
import sqlparse
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
import asyncio
import datetime
import gigachat.context
from rabbitmq import RabbitMQCommunicator  # Импортируем класс для работы с RabbitMQ

class SQLQueryGenerator:
    def __init__(self, config_path='gpt_db/data/confs/confs.json', instruction_path='gpt_db/data/confs/instruction_sql.txt', save_dialog=True, dialog_id="1"):        
        self.api_key = self._load_api_key(config_path)
        self.instruction = self._load_instruction(instruction_path)
        # print(self.instruction)
        self.model = GigaChat(
            credentials=self.api_key,
            scope="GIGACHAT_API_PERS",
            model="GigaChat-2-Max",
            verify_ssl_certs=False,
        )
        self.messages = [
                Messages(
                    role=MessagesRole.SYSTEM,
                    content=self.instruction
                )
        ]
        self.save_dialog = save_dialog
        self.dialog_id = dialog_id
    
        if not os.path.exists('gpt_db/data/dialogs_cash'):
            os.makedirs('gpt_db/data/dialogs_cash')
    
        if os.path.exists(f'gpt_db/data/dialogs_cash/{self.dialog_id}.json'):
            with open(f'gpt_db/data/dialogs_cash/{self.dialog_id}.json', 'r', encoding='utf-8') as f:
                history = json.load(f)
            for h in history[-4:]:
                self.messages += [
                    Messages(
                        role=MessagesRole.USER if h['role'] == 'user' else MessagesRole.ASSISTANT,
                        content=h['content']
                )]

    def _load_api_key(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config['GIGACHAT_CREDENTIALS']

    def _load_instruction(self, instruction_path):
        with open(instruction_path, 'r', encoding='utf-8') as file:
            instruct = file.read().strip()
        with open('gpt_db/data/confs/otgruzki_structure.txt', 'r', encoding='utf-8') as file:
            db_shema = file.read().strip()
        with open('gpt_db/data/confs/divisions.txt', 'r', encoding='utf-8') as file:
            divisions = file.read().strip()
        
        return instruct.replace("[otgruzki_structure]", db_shema).\
            replace("[divizions]", divisions).replace("[date]", str(datetime.datetime.now())[:10])

    def generate_sql_query(self, user_query):
        self.messages +=[
                Messages(
                    role=MessagesRole.USER,
                    content=f"{user_query}\n\n"
                )
            ]
        payload = Chat(
            messages=self.messages,
            temperature=0.01,
            max_tokens=1000
        )
        response = self.model.chat(payload)
        
        if response and response.choices:
            generated_sql = response.choices[0].message.content.strip()
            generated_sql = generated_sql.replace("```sql", "").replace("```", "")
            
            self.messages +=[
                Messages(
                    role=MessagesRole.ASSISTANT,
                    content=generated_sql
                    )
            ]
            # save messages to json file
            if self.save_dialog:
                print("Сохраняем историю")
                history = []
                for m in self.messages:
                    if m.role == MessagesRole.USER:
                        history += [{"role": "user", "content": m.content}]
                    elif m.role == MessagesRole.ASSISTANT:
                        history += [{"role":"assistant", "content": m.content}]
                    # elif m.role == MessagesRole.SYSTEM:
                    #     history += [{"system": m.content}]
                print("History: ", history)
                with open(f'gpt_db/data/dialogs_cash/{self.dialog_id}.json', 'w', encoding='utf-8') as f:
                    # Сохранем список сообщений в файл
                    json.dump(history, f, ensure_ascii=False)
            
            
            

            # Проверяем корректность синтаксиса через sqlparse
            if self._is_sql_valid(generated_sql):
                payload.messages.append(response.choices[0].message)
                return generated_sql
            else:
                raise Exception("Синтаксическая ошибка в сгенерированном SQL-запросе.")
        else:
            raise Exception("Ошибка: не удалось сгенерировать SQL-запрос.")

    def _is_sql_valid(self, sql_query):
        """
        Синтаксическая проверка SQL-запроса с помощью sqlparse.
        Возвращает True, если запрос корректен, иначе False.
        """
        try:
            # Если парсинг прошел успешно, вернется список statement-объектов
            statements = sqlparse.parse(sql_query)
            print("statements - \n", statements)
            # Дополнительно можно проверить, что statements не пуст
            if not statements:
                return False
            return True
        except Exception:
            return False

# Использование RabbitMQ для отправки сгенерированного запроса и получения ответа
async def send_sql_request_to_rabbitmq(user_query, save_dialog=True, dialog_id="1"):
    # Создаем экземпляр RabbitMQCommunicator для отправки сообщений в очередь 'sql_requests'
    communicator = RabbitMQCommunicator("amqp://guest:guest@localhost/", "sql_requests")
    
    # Генерируем SQL-запрос
    sql_generator = SQLQueryGenerator(save_dialog=save_dialog, dialog_id=dialog_id)
    try:
        generated_sql = sql_generator.generate_sql_query(user_query)
        print("Ответ модели:", generated_sql)
        
        # Отправляем сгенерированный SQL-запрос через RabbitMQ
        response = await communicator.call({"user_query": generated_sql})
        response_data = json.loads(response.decode())
        print("Ответ от RabbitMQ-сервиса:", response_data)
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        # Закрываем соединение с RabbitMQ
        await communicator.close()

def main():
    # user_query = "Сколько клиентов отгрузилось на дальнем востоке в прошлом году"
    user_querys = [
        
        "Сколько клиентов отгрузилось на дальнем востоке в прошлом году",
            "Кто из них грузился чаще?",
        "Какой менеджер совершил больше всего продаж в прошлом месяце?",
        "В какой день было больше всего отгрузок?",
        "Скажи материал с самой большой наценкой",
        "Скажи АГ1 с самой большой наценкой",
        "Какой менеджер совершил больше всего продаж в прошлом месяце",
            "посчитай по тоннажу",
        "Покажи отгрузки за сегодня"
    ]
    # Запускаем асинхронную задачу для отправки запроса и получения ответа
    asyncio.run(send_sql_request_to_rabbitmq(user_querys[-1], save_dialog=True, dialog_id="2"))

if __name__ == "__main__":
    main()

# """
# Код дивизиона указывать в комментариях
# Более подробный комментарий, писать какое поле используем
# NULLIF поменять на IFNULL
# Запрос без срока не писать, по умолчанию - текущий месяц
# """