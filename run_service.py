# run_service.py (ФИНАЛЬНАЯ ВЕРСИЯ)

import asyncio
import json
import os
import traceback
from concurrent.futures import ThreadPoolExecutor

import aio_pika
from dotenv import load_dotenv

from gpt_db.agent import GPTAgent

# --- Настройка ---
load_dotenv()
RMQ_URL = os.getenv("RMQ_URL")
EXCHANGE_NAME = os.getenv("RMQ_EXCHANGE_NAME")
INPUT_QUEUE_NAME = os.getenv("RMQ_INPUT_QUEUE")
ROUTING_KEY = os.getenv("RMQ_ROUTING_KEY")

# --- Инициализация ---
print("Инициализация GPT-Агента...")
agent = GPTAgent()
print(">>> Агент успешно инициализирован.")
executor = ThreadPoolExecutor(max_workers=os.cpu_count())


# --- Логика обработки сообщения ---
async def on_message(msg: aio_pika.IncomingMessage):
    async with msg.process():
        loop = asyncio.get_event_loop()
        try:
            request_data = json.loads(msg.body.decode())
            print(f"\n[*] Получен запрос: {request_data.get('message')}")

            response_state = await loop.run_in_executor(
                executor,
                agent.run,
                request_data.get("user_id"),
                request_data.get("message"),
                request_data.get("report_id")
            )

            final_sql, final_comment = None, "Ошибка: Агент не вернул ответ."
            if response_state and response_state.get("messages"):
                last_message = response_state["messages"][-1].content
                if "===" in last_message:
                    parts = last_message.split("===", 1)
                    final_sql, final_comment = parts[0].strip(), parts[1].strip()
                else:
                    final_comment = last_message.strip()

            response_body = {"sql": final_sql, "comment": final_comment}
            print(f"[*] Сформирован ответ: {json.dumps(response_body, ensure_ascii=False)}")

            # ==========================================================
            #           ФИНАЛЬНОЕ ИСПРАВЛЕНИЕ ЗДЕСЬ
            # ==========================================================
            if msg.reply_to and msg.correlation_id:
                # Получаем объект дефолтного обменника из канала
                default_exchange = msg.channel.default_exchange

                # Создаем сообщение для ответа
                response_message = aio_pika.Message(
                    body=json.dumps(response_body).encode('utf-8'),
                    correlation_id=msg.correlation_id,
                    content_type='application/json'
                )

                # Публикуем сообщение через объект ДЕФОЛТНОГО ОБМЕННИКА
                await default_exchange.publish(
                    response_message,
                    routing_key=msg.reply_to,
                )
                print(f"[*] Ответ отправлен в очередь '{msg.reply_to}'")

        except Exception:
            print("\n!!! Произошла критическая ошибка при обработке сообщения:")
            traceback.print_exc()


# --- Основная функция запуска сервиса ---
async def main():
    print(">>> Сервис запускается и подключается к RabbitMQ...")
    connection = await aio_pika.connect_robust(RMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await channel.declare_queue(INPUT_QUEUE_NAME, durable=True)
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        print(f"\n>>> Сервис готов. Ожидание сообщений в очереди '{INPUT_QUEUE_NAME}'.")
        await queue.consume(on_message)
        
        print(">>> Нажмите CTRL+C для выхода.")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n>>> Сервис остановлен вручную.")