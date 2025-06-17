# test_client.py

import asyncio
import json
import os
from dotenv import load_dotenv

from gpt_db.rabbitmq_communicator import RabbitMQCommunicator

# --- Настройка ---
load_dotenv()

# --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
# Определяем URL для локального подключения, игнорируя .env
# Это нужно, так как клиент запущен на хосте, а не внутри Docker-сети.
LOCAL_RMQ_URL = "amqp://admin:admin123@127.0.0.1/"
ROUTING_KEY = os.getenv("RMQ_ROUTING_KEY")
# -------------------------


async def run_test():
    """
    Тестовая функция для отправки запроса и получения ответа.
    """
    print(">>> Запуск тестового клиента...")
    
    # Используем нашу новую переменную для URL
    communicator = RabbitMQCommunicator(
        connection_url=LOCAL_RMQ_URL,
        request_queue=ROUTING_KEY
    )
    
    # Формируем тело запроса
    request_body = {
        "user_id": "user_rmq_001",
        "report_id": "report_rmq_001",
        "message": "Покажи выручку за вчера"
    }

    print(f"\nОтправка запроса:\n{json.dumps(request_body, indent=2, ensure_ascii=False)}")
    
    try:
        # Вызываем метод call, который отправит сообщение и будет ждать ответа
        # Установим таймаут, на случай если сервис не отвечает
        response_body_bytes = await asyncio.wait_for(communicator.call(request_body), timeout=60.0)
        
        # Декодируем и выводим ответ
        response_data = json.loads(response_body_bytes.decode())
        
        print("\n--- Получен ответ ---")
        print(json.dumps(response_data, indent=2, ensure_ascii=False))
        print("--------------------")

    except asyncio.TimeoutError:
        print("\n!!! Ошибка: Таймаут. Сервис не ответил за 60 секунд.")
    except Exception as e:
        print(f"\n!!! Произошла ошибка: {e}")
    finally:
        await communicator.close()
        print("\n>>> Тестовый клиент завершил работу.")


if __name__ == "__main__":
    asyncio.run(run_test())