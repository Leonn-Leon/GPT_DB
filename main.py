import streamlit as st
import os
import yaml # Используется в GPTAgent, нужен для инициализации путей
from dotenv import load_dotenv # Используется в GPTAgent

# Импортируем ваш класс GPTAgent
# Предполагается, что ваш скрипт сохранен как my_agent_module.py
# в той же директории, что и этот streamlit_app.py
from gpt_db.agent import GPTAgent 

# --- Конфигурация путей (скопировано из вашего __main__ блока) ---
# Это нужно, чтобы агент мог найти свои файлы конфигурации.
# Убедитесь, что эти пути корректны относительно места запуска Streamlit.
# Обычно Streamlit запускается из корня проекта.

DATA_DIR = os.path.join("gpt_db", "data")
CONF_DIR = os.path.join(DATA_DIR, "confs")
DIALOG_CASH_DIR = os.path.join(DATA_DIR, "dialogs_cash") # Убедитесь, что этот путь соответствует вашей структуре

CONFIG_FILE = os.path.join(CONF_DIR, "config.yaml")
STRUCTURE_FILE = os.path.join(CONF_DIR, 'otgruzki_structure.txt')
DIVISIONS_FILE = os.path.join(CONF_DIR, 'divisions.txt')
BASE_HISTORY_FILE = os.path.join(DIALOG_CASH_DIR, "history_base.json")
AUTHORITY_DB_FILE = os.path.join(DATA_DIR, 'authority.csv')
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
            CONFIG_FILE: {"GIGACHAT_CREDENTIALS": "YOUR_GIGACHAT_API_KEY_HERE"},
            STRUCTURE_FILE: "FKDAT (DATE): Дата фактуры\nNETWR (DECIMAL): Чистая стоимость",
            DIVISIONS_FILE: "100: Центр",
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

        if not os.path.exists(AUTHORITY_DB_FILE):
            st.error(f"КРИТИЧЕСКАЯ ОШИБКА: Файл БД авторизации '{AUTHORITY_DB_FILE}' не найден. Агент не сможет применять ограничения.")
            # Можно создать пустую БД SQLite, но это не даст функциональности ограничений
            # Для демонстрации можно продолжить, но агент будет работать некорректно с ограничениями
            # return None # Раскомментируйте, если хотите остановить приложение при отсутствии БД прав

        # Загрузка .env файла, если он есть
        load_dotenv()

        agent = GPTAgent(
            config_file=CONFIG_FILE,
            structure_file=STRUCTURE_FILE,
            divisions_file=DIVISIONS_FILE,
            base_history_file=BASE_HISTORY_FILE,
            authority_db_path=AUTHORITY_DB_FILE,
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

st.info(
    "**Важно:** Если агент запрашивает уточнения, вам нужно будет ввести их "
    "**в консоли (терминале), где запущен Streamlit**, а не в этом веб-интерфейсе. "
    "После ввода в консоли, агент продолжит работу, и результат отобразится здесь."
)

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
        with st.spinner("Агент думает... (проверьте консоль, если ожидается уточнение)"):
            try:
                # Запускаем агент. `run` возвращает `final_state_values`
                # final_state_values['messages'] содержит сообщения этого раунда графа
                # Последнее сообщение - это комментарий.
                # Предпоследнее (если есть и корректное) - это SQL.
                response_state = agent.run(user_id=user_id, message=prompt, report_id=report_id)

                generated_sql = None
                agent_comment = "Не удалось получить комментарий от агента."

                if response_state and response_state.get('messages'):
                    graph_messages = response_state['messages']
                    if graph_messages:
                        # Последнее сообщение - это комментарий или сообщение об ошибке
                        agent_comment = graph_messages[-1].content.split("===")[1]

                        # Ищем SQL: он должен быть предпоследним сообщением AIMessage,
                        # если все прошло успешно до этапа комментирования.
                        if len(graph_messages) > 1 and hasattr(graph_messages[-2], 'content'):
                            potential_sql = graph_messages[-2].content
                            # Простая проверка, что это похоже на SQL, а не на сообщение об ошибке/пропуске
                            # или инструкцию "ok" от валидатора
                            sql_keywords = ["SELECT ", "WITH ", "select ", "with "] # Case-sensitive from GigaChat
                            if any(keyword in potential_sql for keyword in sql_keywords) and \
                                "sql generation skipped" not in potential_sql.lower() and \
                                "ошибка при генерации sql" not in potential_sql.lower() and \
                                not potential_sql.lower().startswith("ok\n"):
                                generated_sql = potential_sql

                # Добавляем ответ агента в историю и отображаем
                assistant_message = {"role": "assistant", "content": agent_comment, "sql": generated_sql}
                st.session_state.messages.append(assistant_message)
                with st.chat_message("assistant"):
                    st.markdown(agent_comment)
                    if generated_sql:
                        st.code(generated_sql, language="sql")
                    elif agent_comment and "sql generation skipped" not in agent_comment.lower() and "ошибка" not in agent_comment.lower():
                        st.info("SQL-запрос не был явно извлечен для отображения, но агент мог его сгенерировать и использовать.")


            except Exception as e:
                st.error(f"Ошибка при вызове агента: {e}")
                import traceback
                st.text(traceback.format_exc())
                # Добавляем сообщение об ошибке в чат
                error_message_for_chat = f"Произошла ошибка: {e}"
                st.session_state.messages.append({"role": "assistant", "content": error_message_for_chat, "sql": None})
                with st.chat_message("assistant"):
                    st.markdown(error_message_for_chat)

# --- Кнопка для очистки истории чата и кеша агента ---
if st.sidebar.button("Очистить чат и перезагрузить агента"):
    st.session_state.messages = []
    st.cache_resource.clear() # Очищаем кеш, чтобы агент перезагрузился
    st.rerun()