import os
import json
import sqlparse
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
import asyncio
from rabbitmq import RabbitMQCommunicator  # Импортируем класс для работы с RabbitMQ

class SQLQueryGenerator:
    def __init__(self, config_path='gpt_db/data/confs/confs.json', instruction_path='gpt_db/data/confs/instruction_sql.txt'):
        self.api_key = self._load_api_key(config_path)
        self.instruction = self._load_instruction(instruction_path)
        self.model = GigaChat(
            credentials=self.api_key,
            scope="GIGACHAT_API_PERS",
            model="GigaChat-Max",
            verify_ssl_certs=False,
        )

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
            
        return instruct.replace("[otgruzki_structure]", db_shema).replace("[divisions]", divisions)

    def generate_sql_query(self, user_query):
        payload = Chat(
            messages = [
                Messages(
                    role=MessagesRole.SYSTEM,
                    content=self.instruction
                ),
                Messages(
                    role=MessagesRole.USER,
                    content=f"Запрос пользователя:\n{user_query}\n\nСоставь соответствующий SQL-запрос."
                )
            ],
            temperature=0.01,
            max_tokens=1000
        )
        response = self.model.chat(payload)
        
        if response and response.choices:
            generated_sql = response.choices[0].message.content.strip()
            generated_sql = generated_sql.replace("```sql", "").replace("```", "")

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
async def send_sql_request_to_rabbitmq(user_query):
    # Создаем экземпляр RabbitMQCommunicator для отправки сообщений в очередь 'sql_requests'
    communicator = RabbitMQCommunicator("amqp://guest:guest@localhost/", "sql_requests")
    
    # Генерируем SQL-запрос
    sql_generator = SQLQueryGenerator()
    try:
        generated_sql = sql_generator.generate_sql_query(user_query)
        print("Сгенерированный SQL-запрос:\n", generated_sql)
        
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
    user_query = "Назови топ 3 дивизиона за прошлый месяц"
    
    # Запускаем асинхронную задачу для отправки запроса и получения ответа
    asyncio.run(send_sql_request_to_rabbitmq(user_query))

if __name__ == "__main__":
    main()
