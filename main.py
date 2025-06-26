from dotenv import load_dotenv
import streamlit as st
import os
import yaml
import uuid
import traceback
import asyncio
import threading # Для фонового потока
import aio_pika  # Для RabbitMQ
import json
from concurrent.futures import ThreadPoolExecutor

from gpt_db.agent import GPTAgent

# --- КОНФИГУРАЦИЯ RABBITMQ ---
RMQ_URL = os.getenv("RMQ_URL")
EXCHANGE_NAME = os.getenv("RMQ_EXCHANGE_NAME")
INPUT_QUEUE_NAME = os.getenv("RMQ_INPUT_QUEUE")
ROUTING_KEY = os.getenv("RMQ_ROUTING_KEY")

# Пул потоков для выполнения синхронного кода агента в асинхронной среде
executor = ThreadPoolExecutor(max_workers=os.cpu_count())

class RabbitMQService:
    def __init__(self, agent_instance: GPTAgent):
        self.agent = agent_instance
        self.loop = None
        self.connection = None

    async def on_message(self, msg: aio_pika.IncomingMessage):
        """Обработчик входящих сообщений (почти без изменений)."""
        async with msg.process():
            try:
                request_data = json.loads(msg.body.decode())
                user_id = request_data.get("user_id", "default_user")
                report_id = request_data.get("report_id", "default_report")
                message = request_data.get("message", "")
                print(f"[RabbitMQ] Получен запрос от user='{user_id}': '{message}'")

                # Вызываем синхронный метод агента в отдельном потоке
                response_state = await self.loop.run_in_executor(
                    executor, self.agent.run, user_id, message, report_id
                )

                # Форматируем ответ
                final_sql = None
                final_comment = "Произошла ошибка или агент не дал ответа."
                if response_state and response_state.get("messages"):
                    last_message_obj = response_state["messages"][-1]
                    if "===" in last_message_obj.content:
                        parts = last_message_obj.content.split("===", 1)
                        final_sql, final_comment = parts[0].strip(), parts[1].strip()
                    else:
                        final_sql, final_comment = None, last_message_obj.content.strip()

                response_body = {"sql": final_sql, "comment": final_comment}
                print(f"[RabbitMQ] Сформирован ответ: {json.dumps(response_body, ensure_ascii=False)}")

                # Отправляем ответ обратно
                if msg.reply_to and msg.correlation_id:
                # Получаем канал из текущего соединения
                    channel = await self.connection.channel()

                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=json.dumps(response_body).encode('utf-8'),
                            correlation_id=msg.correlation_id,
                            content_type='application/json'
                        ),
                        routing_key=msg.reply_to,
                    )
                    print(f"[RabbitMQ] Ответ отправлен в очередь '{msg.reply_to}'")
                    
                    await channel.close()

            except Exception:
                print("\n[RabbitMQ] !!! Произошла критическая ошибка при обработке сообщения:")
                traceback.print_exc()

    async def start_listening(self):
        """Основной метод, который запускается в фоновом потоке."""
        print("[RabbitMQ] Слушатель запускается в фоновом потоке...")
        self.loop = asyncio.get_event_loop()
        self.connection = await aio_pika.connect_robust(RMQ_URL, loop=self.loop)
        
        async with self.connection:
            channel = await self.connection.channel()
            await channel.set_qos(prefetch_count=1)
            exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True)
            queue = await channel.declare_queue(INPUT_QUEUE_NAME, durable=True)
            await queue.bind(exchange, routing_key=ROUTING_KEY)

            print(f"\n>>> [RabbitMQ] Сервис готов. Ожидание сообщений в очереди '{INPUT_QUEUE_NAME}'.")
            await queue.consume(self.on_message)
            
            # Держим поток в рабочем состоянии
            await asyncio.Future()

    def run_in_background(self):
        """
        Запускает асинхронный слушатель в новом потоке,
        правильно управляя циклом событий asyncio.
        """
        # Создаем новый цикл событий специально для этого потока
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Запускаем наш основной асинхронный метод в этом цикле
            loop.run_until_complete(self.start_listening())
        except Exception as e:
            print(f"[RabbitMQ] Критическая ошибка в фоновом потоке: {e}")
            traceback.print_exc()
        finally:
            # Корректно закрываем цикл после завершения
            loop.close()
        
        
# --- Константы путей (без изменений) ---
DATA_DIR = os.path.join("gpt_db", "data")
CONF_DIR = os.path.join(DATA_DIR, "confs")
DIALOG_CASH_DIR = os.path.join(DATA_DIR, "dialogs_cash")

CONFIG_FILE = os.path.join(CONF_DIR, "config.yaml")
STRUCTURE_FILE = os.path.join(CONF_DIR, 'otgruzki_structure.yaml')
DIVISIONS_FILE = os.path.join(CONF_DIR, 'divisions.txt')
CHECKPOINT_DB_FILE = os.path.join(DATA_DIR, "checkpoints.sqlite")

# --- Функция для загрузки и кеширования агента (без изменений) ---
@st.cache_resource
def load_gpt_agent():
    try:
        os.makedirs(CONF_DIR, exist_ok=True)
        if DIALOG_CASH_DIR != DATA_DIR:
             os.makedirs(DIALOG_CASH_DIR, exist_ok=True)

        load_dotenv()

        agent = GPTAgent(
            config_file=CONFIG_FILE,
            structure_file=STRUCTURE_FILE,
            divisions_file=DIVISIONS_FILE,
            checkpoint_db=CHECKPOINT_DB_FILE
        )
        st.success("Агент успешно инициализирован.")
        return agent
    except Exception as e:
        st.error(f"Ошибка при инициализации GPTAgent: {e}")
        st.text(traceback.format_exc())
        return None

# --- Основной интерфейс Streamlit ---
st.title("Чат с SQL-Агентом")

agent = load_gpt_agent()

if 'rabbitmq_service_started' not in st.session_state:
    if agent is not None:
        print(">>> Попытка запуска RabbitMQ сервиса в фоновом потоке...")
        # Создаем экземпляр нашего сервиса, передавая ему уже созданного агента
        rabbitmq_service = RabbitMQService(agent_instance=agent)
        
        # Создаем и запускаем фоновый поток
        thread = threading.Thread(target=rabbitmq_service.run_in_background, daemon=True)
        thread.start()
        
        # Ставим флаг, что сервис запущен
        st.session_state.rabbitmq_service_started = True
        st.success("Фоновый сервис RabbitMQ запущен!")
    else:
        st.error("Агент не инициализирован, фоновый сервис RabbitMQ не может быть запущен.")
        
        
if agent is None:
    st.warning("Агент не был загружен. Приложение не может продолжить работу.")
    st.stop()

# --- Боковая панель для управления сессией ---
with st.sidebar:
    st.header("Параметры сессии")
    user_id = st.text_input("ID пользователя (user_id)", value="user0")

    # Инициализируем report_id в состоянии сессии, если его там нет
    if "report_id" not in st.session_state:
        st.session_state.report_id = str(uuid.uuid4())

    # Отображаем текущий ID отчета (нередактируемый)
    st.text_input("ID текущего диалога", value=st.session_state.report_id, disabled=True)
    report_id = st.session_state.report_id

    if not user_id:
        st.warning("Пожалуйста, введите ID пользователя.")

    # ИЗМЕНЕНИЕ: Теперь у нас одна, правильная кнопка для сброса
    if st.button("Начать новый диалог"):
        st.session_state.messages = []
        st.session_state.report_id = str(uuid.uuid4())
        # st.cache_resource.clear() больше не нужен, т.к. агент один на все диалоги
        st.rerun()

# --- Логика чата ---
# Инициализация истории чата
if "messages" not in st.session_state:
    st.session_state.messages = []

# Отображение истории чата
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message and message["sql"]:
            st.code(message["sql"], language="sql")

# Поле ввода пользователя
if prompt := st.chat_input("Ваш вопрос к БД отгрузок:"):
    if not user_id or not report_id:
        st.warning("Пожалуйста, заполните ID пользователя и ID отчета в боковой панели.")
    else:
        # ИЗМЕНЕНИЕ: Добавляем "sql": None для единообразия структуры
        st.session_state.messages.append({"role": "user", "content": prompt, "sql": None})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Получаем ответ от агента
        with st.chat_message("assistant"):
            with st.spinner("Агент думает..."):
                try:
                    response_state = agent.run(user_id=user_id, message=prompt, report_id=report_id)
                    last = response_state["messages"][-1] if response_state and response_state.get("messages") else None

                    if last:
                        # ИЗМЕНЕНИЕ: Более надежная проверка на наличие SQL в ответе
                        if "===" in last.content:
                            # Используем split с ограничением, чтобы избежать ошибок
                            parts = last.content.split("===", 1)
                            sql_content = parts[0].strip()
                            comment_content = parts[1].strip()

                            st.code(sql_content, language="sql")
                            st.markdown(comment_content)

                            # Сохраняем обе части в историю
                            assistant_message = {"role": "assistant", "content": comment_content, "sql": sql_content}
                            st.session_state.messages.append(assistant_message)
                        else:
                            # Если разделителя нет, это обычное сообщение (например, уточняющий вопрос)
                            st.markdown(last.content.strip())
                            assistant_message = {"role": "assistant", "content": last.content.strip(), "sql": None}
                            st.session_state.messages.append(assistant_message)

                except Exception as e:
                    st.error(f"Ошибка при вызове агента: {e}")
                    st.code(traceback.format_exc())
                    error_message_for_chat = f"Произошла ошибка: {e}"
                    st.session_state.messages.append({"role": "assistant", "content": error_message_for_chat, "sql": None})
                    st.markdown(error_message_for_chat)