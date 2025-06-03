import streamlit as st
import os
import yaml # Используется в GPTAgent, нужен для инициализации путей
from dotenv import load_dotenv # Используется в GPTAgent

from gpt_db.agent import GPTAgent 

DATA_DIR = os.path.join("gpt_db", "data")
CONF_DIR = os.path.join(DATA_DIR, "confs")
DIALOG_CASH_DIR = os.path.join(DATA_DIR, "dialogs_cash") # Убедитесь, что этот путь соответствует вашей структуре

CONFIG_FILE = os.path.join(CONF_DIR, "config.yaml")
STRUCTURE_FILE = os.path.join(CONF_DIR, 'otgruzki_structure.yaml')
DIVISIONS_FILE = os.path.join(CONF_DIR, 'divisions.txt')
BASE_HISTORY_FILE = os.path.join(DIALOG_CASH_DIR, "history_base.json")
CHECKPOINT_DB_FILE = os.path.join(DATA_DIR, "checkpoints.sqlite")

# --- Функция для загрузки и кеширования агента ---
@st.cache_resource # Кешируем ресурс, чтобы не инициализировать агента при каждом действии
def load_gpt_agent():
    try:
        # Создаем директории, если их нет (упрощенная версия вашего setup)
        os.makedirs(CONF_DIR, exist_ok=True)
        if DIALOG_CASH_DIR != DATA_DIR : # Создаем, только если отличается
             os.makedirs(DIALOG_CASH_DIR, exist_ok=True)

        # Проверка и создание минимальных конфигурационных файлов, если их нет
        # Это упрощенная версия, чтобы избежать падения агента при первом запуске
        # В идеале, эти файлы должны быть подготовлены заранее.
        dummy_files = {
            BASE_HISTORY_FILE: "" # Пустой файл истории
        }
        for filepath, content in dummy_files.items():
            if not os.path.exists(filepath):
                try:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        if isinstance(content, dict): yaml.dump(content, f)
                        else: f.write(str(content))
                    st.warning(f"Файл '{filepath}' не найден, создан пустой/демо файл. Проверьте его содержимое.")
                except Exception as e_file:
                    st.error(f"Не удалось создать файл '{filepath}': {e_file}")
                    return None # Не можем продолжить без критических файлов

        load_dotenv()

        agent = GPTAgent(
            config_file=CONFIG_FILE,
            structure_file=STRUCTURE_FILE,
            divisions_file=DIVISIONS_FILE,
            base_history_file=BASE_HISTORY_FILE,
            checkpoint_db=CHECKPOINT_DB_FILE
        )
        st.success("Агент успешно инициализирован.")
        return agent
    except Exception as e:
        st.error(f"Ошибка при инициализации GPTAgent: {e}")
        import traceback
        st.text(traceback.format_exc())
        return None

# --- Основной интерфейс Streamlit ---
st.title("Чат с SQL-Агентом")

agent = load_gpt_agent()

# --- Боковая панель для ввода ID пользователя и отчета ---
st.sidebar.header("Параметры сессии")
user_id = st.sidebar.text_input("ID пользователя (user_id)", value="user0")
report_id = st.sidebar.text_input("ID отчета (report_id)", value="default_report")

if not user_id:
    st.sidebar.warning("Пожалуйста, введите ID пользователя.")
if not report_id:
    st.sidebar.warning("Пожалуйста, введите ID отчета.")

# --- Инициализация истории чата в session_state ---
if "messages" not in st.session_state:
    st.session_state.messages = []

# --- Отображение истории чата ---
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message and message["sql"]:
            st.code(message["sql"], language="sql")

# --- Поле ввода пользователя ---
if prompt := st.chat_input("Ваш вопрос к БД отгрузок:"):
    if not user_id or not report_id:
        st.warning("Пожалуйста, заполните ID пользователя и ID отчета в боковой панели.")
    else:
        # Добавляем сообщение пользователя в историю и отображаем
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Получаем ответ от агента
        with st.spinner("Агент думает..."):
            try:
                response_state = agent.run(user_id=user_id, message=prompt, report_id=report_id)
                last = response_state["messages"][-1] if response_state and response_state.get("messages") else None

                if last:
                    with st.chat_message("assistant"):
                        if "select" in last.content.lower() and "from" in last.content.lower():
                            sql_content = last.content.split("===")[0]
                            comment_content = last.content.split("===")[1]
                            st.code(sql_content, language="sql")
                            st.markdown(comment_content.strip())
                            assistant_message = {"role": "assistant", "content": comment_content, "sql": sql_content}
                            st.session_state.messages.append(assistant_message)
                        else:
                            st.markdown(last.content.strip())
                            assistant_message = {"role": "assistant", "content": last.content.strip(), "sql": None}
                            st.session_state.messages.append(assistant_message)

            except Exception as e:
                st.error(f"Ошибка при вызове агента: {e}")
                import traceback
                st.text(traceback.format_exc())
                # Добавляем сообщение об ошибке в чат
                error_message_for_chat = f"Произошла ошибка: {e}"
                st.session_state.messages.append({"role": "assistant", "content": error_message_for_chat, "sql": None})
                with st.chat_message("assistant"):
                    st.markdown(error_message_for_chat)

if st.sidebar.button("Очистить чат и перезагрузить агента"):
    st.session_state.messages = []
    st.cache_resource.clear() # Очищаем кеш, чтобы агент перезагрузился
    st.rerun()