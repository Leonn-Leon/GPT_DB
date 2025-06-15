from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import os
import yaml
import uuid
import traceback # ИЗМЕНЕНИЕ: импорт перенесен наверх для чистоты кода

from gpt_db.agent import GPTAgent

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