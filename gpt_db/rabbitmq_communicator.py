# rabbitmq_communicator.py
import aio_pika
import asyncio
import uuid
import json
import os

class RabbitMQCommunicator:
    """
    Класс для общения с RabbitMQ посредством RPC-подобного взаимодействия.
    Позволяет отправлять запросы в указанную очередь и получать ответы через callback-очередь.
    """
    def __init__(self, connection_url: str, request_queue: str):
        self.connection_url = connection_url
        self.request_queue = request_queue
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.futures = {}

    async def connect(self):
        """
        Устанавливает соединение с RabbitMQ и настраивает callback-очередь для получения ответов.
        """
        self.connection = await aio_pika.connect_robust(self.connection_url)
        self.channel = await self.connection.channel()

        # Создаем временную (анонимную) очередь для получения ответов
        callback_queue = await self.channel.declare_queue('', exclusive=True)
        self.callback_queue = callback_queue.name

        # Устанавливаем QoS, чтобы обрабатывать по одному сообщению за раз
        await self.channel.set_qos(prefetch_count=1)

        # Подписываемся на callback-очередь
        await callback_queue.consume(self.on_response)

    async def on_response(self, msg: aio_pika.IncomingMessage):
        """
        Обработчик входящих сообщений из callback-очереди.
        Если найден ожидающий запрос с таким correlation_id, возвращает ответ.
        """
        if msg.correlation_id in self.futures:
            future = self.futures.pop(msg.correlation_id)
            future.set_result(msg.body)

    async def call(self, message_body: dict) -> bytes:
        """
        Отправляет запрос в указанную очередь и ожидает ответа.
        
        Args:
            message_body (dict): Данные запроса, которые будут сериализованы в JSON.
        
        Returns:
            bytes: Тело ответа, полученное от сервиса.
        """
        if self.connection is None:
            await self.connect()

        correlation_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.futures[correlation_id] = future

        message = aio_pika.Message(
            body=json.dumps(message_body, ensure_ascii=False).encode('utf-8'),
            reply_to=self.callback_queue,
            correlation_id=correlation_id,
            content_type='application/json'
        )
        
        # 1. Получаем наш именованный обменник из .env
        exchange = await self.channel.get_exchange(os.getenv("RMQ_EXCHANGE_NAME"))
        
        # 2. Публикуем сообщение в него, используя request_queue как routing_key
        await exchange.publish(
            message,
            routing_key=self.request_queue
        )
        return await future

    async def close(self):
        """
        Закрывает соединение с RabbitMQ.
        """
        if self.connection:
            await self.connection.close()