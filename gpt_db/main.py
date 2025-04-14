import os
import json
import datetime
import yaml
from typing import List, Annotated, Union, Dict
from typing_extensions import TypedDict

# --- Langchain & Langgraph Imports ---
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph.message import add_messages

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, BaseMessage
from langchain_gigachat.chat_models import GigaChat

# --- Константы и Настройки ---
BASE_HISTORY_FILE = "history_base.json"
CHECKPOINT_DB = "checkpoints.sqlite"
CONFIG_FILE = "gpt_db/data/confs/config.yaml"
STRUCTURE_FILE = 'gpt_db/data/confs/otgruzki_structure.txt'
DIVISIONS_FILE = 'gpt_db/data/confs/divisions.txt'

# --- Загрузка конфигурации и данных ---

def _load_config_and_data():
    """Загружает конфигурацию GigaChat, схему БД и дивизионы."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        gigachat_credentials = config["GIGACHAT_CREDENTIALS"]
    except FileNotFoundError:
        print(f"Ошибка: Файл конфигурации '{CONFIG_FILE}' не найден.")
        exit(1)
    except KeyError:
        print(f"Ошибка: Ключ 'GIGACHAT_CREDENTIALS' не найден в '{CONFIG_FILE}'.")
        exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла конфигурации '{CONFIG_FILE}': {e}")
        exit(1)

    try:
        with open(STRUCTURE_FILE, 'r', encoding='utf-8') as file:
            db_schema = file.read().strip()
    except FileNotFoundError:
        print(f"Ошибка: Файл схемы '{STRUCTURE_FILE}' не найден.")
        exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла схемы '{STRUCTURE_FILE}': {e}")
        exit(1)

    try:
        with open(DIVISIONS_FILE, 'r', encoding='utf-8') as file:
            divisions_data = file.read().strip()
    except FileNotFoundError:
        print(f"Ошибка: Файл дивизионов '{DIVISIONS_FILE}' не найден.")
        exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла дивизионов '{DIVISIONS_FILE}': {e}")
        exit(1)

    return gigachat_credentials, db_schema, divisions_data

# Загружаем данные один раз
GIGACHAT_CREDENTIALS, otgruzki_structure, divisions = _load_config_and_data()

# --- Инициализация LLM ---
try:
    llm = GigaChat(
        credentials=GIGACHAT_CREDENTIALS,
        model="GigaChat-2-Max", # Используем Pro для более сложных задач SQL/анализа
        verify_ssl_certs=False, # Оставьте False, если есть проблемы с SSL-сертификатами
        temperature=0.01,       # Низкая температура для более детерминированных ответов
        timeout=600             # Увеличим таймаут на всякий случай
    )
    # Простая проверка доступности модели
    # llm.invoke("Привет!")
    print("GigaChat LLM инициализирован успешно.")
except Exception as e:
    print(f"Ошибка при инициализации GigaChat: {e}")
    print("Убедитесь, что GIGACHAT_CREDENTIALS верны и модель доступна.")
    exit(1)


# --- Утилиты для истории ---

def load_base_history(filename: str = BASE_HISTORY_FILE) -> list[BaseMessage]:
    """
    Загружает базовую историю из JSON-файла (один JSON объект на строку).
    """
    messages = []
    if not os.path.exists(filename):
        print(f"Предупреждение: Файл базовой истории '{filename}' не найден. Будет создан пустой.")
        # Создаем пустой файл, если его нет
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                pass # Просто создаем файл
        except Exception as e:
             print(f"Не удалось создать файл базовой истории '{filename}': {e}")
        return messages # Возвращаем пустой список

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    msg_type = record.get("type")
                    content = record.get("content", "")

                    if msg_type == "human_answer" or msg_type == "human":
                        messages.append(HumanMessage(content=content))
                    elif msg_type == "agent_answer" or msg_type == "ai":
                        messages.append(AIMessage(content=content))
                    elif msg_type == "system":
                         messages.append(SystemMessage(content=content))
                    else:
                        print(f"Предупреждение: Неизвестный тип сообщения '{msg_type}' в строке {i+1} файла '{filename}'.")

                except json.JSONDecodeError as e:
                    print(f"Ошибка декодирования JSON в строке {i+1} файла '{filename}': {e}")
                except Exception as e:
                    print(f"Ошибка обработки строки {i+1} файла '{filename}': {e}")

    except Exception as e:
        print(f"Неожиданная ошибка при загрузке базовой истории '{filename}': {e}")

    return messages

# Загружаем базовую историю один раз при старте
print(f"Загрузка базовой истории из {BASE_HISTORY_FILE}...")
base_history_messages = load_base_history(BASE_HISTORY_FILE)
print(f"Загружено {len(base_history_messages)} сообщений базовой истории.")

# --- Определение состояния графа ---

class MessagesState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    # Добавим поле для хранения финальной инструкции, чтобы не искать ее в сообщениях
    final_instruction: str | None

# --- Узлы графа ---

def validate_instruction(state: MessagesState) -> Dict[str, Union[List[BaseMessage], str, None]]:
    """
    Узел валидации инструкции. Взаимодействует с пользователем для уточнений.
    """
    current_messages = state['messages']
    last_user_message = current_messages[-1].content.strip()

    print(f"\n--- Узел: validate_instruction ---")
    print(f"Получено сообщение: {last_user_message}")

    sys_msg_content = (
        f"Описание структуры БД:\n{otgruzki_structure}\n\n"
        f"Справочник дивизионов:\n{divisions}\n"
        f"Сегодняшняя дата: {datetime.date.today().strftime('%Y%m%d')}\n\n"
        "Ты ассистент, помогающий сформулировать точный запрос к базе данных отгрузок. Твоя задача - валидировать и уточнять запрос пользователя.\n"
        "Правила валидации:\n"
        "1. Проверь, соответствует ли запрос доступным полям в структуре БД. Если пользователь упоминает несуществующие поля, укажи на это.\n"
        "2. Убедись, что из запроса ЯСНО, какие КОНКРЕТНЫЕ ПОЛЯ (столбцы) нужно вывести. Запрос 'покажи отгрузки' невалиден - нужно уточнить, ЧТО именно показать (например, 'покажи чистую стоимость и количество'). Запрещено выводить все поля (`SELECT *`).\n"
        "3. Проверь, понятны ли фильтры (даты, дивизионы, клиенты и т.д.). Если период не указан, уточни (нельзя запрашивать данные за все время).\n"
        "4. Если что-то неясно или не соответствует правилам, задай КОРОТКИЙ уточняющий вопрос. Предлагай варианты, если это уместно (например, 'Уточните, какие поля вывести: количество, стоимость или оба?').\n"
        "5. Если запрос пользователя ПОЛНОСТЬЮ ясен, точен и соответствует правилам, ответь СТРОГО в формате:\n"
        "ok\n<Здесь четко сформулированная итоговая инструкция для генерации SQL>\n"
        "Пример ответа 'ok':\nok\nПокажи сумму чистой стоимости (NETWR) и количество фактур (FKIMG) для дивизиона '100' за вчерашний день ({datetime.date.today() - datetime.timedelta(days=1):%Y%m%d}).\n"
        "ВАЖНО: Поле даты ФАКТУРЫ (FKDAT) в таблице ЕСТЬ. Не говори, что его нет.\n"
        "Отвечай кратко: либо уточняющий вопрос, либо 'ok' с итоговой инструкцией."
    )

    # Формируем историю для LLM: системное сообщение + текущая история диалога
    conversation_for_llm = [SystemMessage(content=sys_msg_content)] + current_messages

    while True:
        print("\nВызов LLM для валидации...")
        try:
            response = llm.invoke(conversation_for_llm)
            result_text = response.content.strip()
            print(f"Ответ LLM (валидация): {result_text}")
        except Exception as e:
            print(f"Ошибка при вызове LLM в validate_instruction: {e}")
            # Возвращаем сообщение об ошибке, чтобы граф мог завершиться
            return {"messages": [AIMessage(content=f"Произошла ошибка при обращении к языковой модели: {e}")]}

        lower_text = result_text.lower()
        if lower_text.startswith("ok"):
            # Инструкция принята
            parts = result_text.split('\n', 1)
            final_instruction = parts[1].strip() if len(parts) > 1 else "Инструкция не извлечена после 'ok'"
            print(f"✅ Инструкция принята: {final_instruction}")

            # Возвращаем сообщение "ok..." и сохраняем инструкцию в state
            return {
                "messages": [AIMessage(content=result_text)], # Сообщение "ok, вот инструкция..."
                "final_instruction": final_instruction
            }
        else:
            # Модель просит уточнений
            print(f"⚠️ Уточнение от модели: {result_text}")
            try:
                # Добавляем вопрос модели в историю для следующего шага
                ai_question_message = AIMessage(content=result_text)
                conversation_for_llm.append(ai_question_message)

                clarification = input(f"🔄 [{datetime.datetime.now().strftime('%H:%M:%S')}] Введите уточнение (или 'stop' для выхода):\n{result_text}\n> ")
                clarification = clarification.strip()

                if clarification.lower() == 'stop' or not clarification:
                    print("Прервано пользователем.")
                    # Возвращаем сообщение об отмене
                    return {
                        "messages": [ai_question_message, HumanMessage(content=clarification), AIMessage(content="Операция отменена пользователем.")],
                        "final_instruction": None # Сбрасываем инструкцию
                        }

                # Добавляем ответ пользователя в историю для следующего вызова LLM
                user_clarification_message = HumanMessage(content=clarification)
                conversation_for_llm.append(user_clarification_message)
                # Эти сообщения (ai_question_message, user_clarification_message)
                # нужно будет вернуть из узла, чтобы они сохранились в общем состоянии,
                # но мы сделаем это только когда цикл завершится (либо через 'ok', либо через 'stop').
                # Пока они живут только в локальной `conversation_for_llm`.

            except EOFError: # Если запускается не интерактивно
                 print("Ошибка ввода (EOF), прерывание.")
                 return {
                     "messages": [AIMessage(content="Произошла ошибка ввода, операция прервана.")],
                     "final_instruction": None
                     }
            except Exception as e:
                 print(f"Неожиданная ошибка ввода: {e}")
                 return {
                     "messages": [AIMessage(content=f"Произошла ошибка обработки ввода: {e}")],
                     "final_instruction": None
                     }


def generate_sql_query(state: MessagesState) -> Dict[str, Union[List[BaseMessage], str, None]]:
    """
    Генерирует SQL-запрос на основе валидированной инструкции из state.
    """
    print(f"\n--- Узел: generate_sql_query ---")
    validated_instruction = state.get('final_instruction')

    if not validated_instruction:
         print("Ошибка: Валидированная инструкция отсутствует в состоянии.")
         # Если инструкция None (например, после 'stop' в валидации), не генерируем SQL
         # Просто передаем управление дальше или можно завершить граф здесь.
         # В данном случае, просто вернем пустое сообщение, comment_sql_query обработает.
         return {"messages": [AIMessage(content="-- SQL generation skipped (no instruction) --")]}

    print(f"Инструкция для генерации SQL: {validated_instruction}")

    sys_msg_content = (
        "Ты – эксперт по SQL (HANA) и анализу данных. Твоя задача - написать ТОЧНЫЙ и ОПТИМАЛЬНЫЙ SQL-запрос к базе данных SAP HANA.\n"
        f"ЗАПРОСЫ ИДУТ ТОЛЬКО К ТАБЛИЦЕ: SAPABAP1.ZZSDM_117_CUS\n"
        f"Её структура:\n{otgruzki_structure}\n\n"
        f"Справочник дивизионов (используй коды в запросе):\n{divisions}\n\n"
        f"Сегодняшняя дата: {datetime.date.today().strftime('%Y%m%d')}\n\n"
        "СТРОГИЕ ПРАВИЛА ГЕНЕРАЦИИ SQL:\n"
        "1.  Используй ТОЛЬКО поля из предоставленной структуры таблицы `SAPABAP1.ZZSDM_117_CUS`.\n"
        "2.  Для полей-характеристик (текстовые, даты, коды) используй `GROUP BY`.\n"
        "3.  Для полей-показателей (числовые: NETWR, FKIMG, ZZACOST, ZZMARG) используй агрегатные функции (`SUM`, `COUNT`, `AVG`). `COUNT(DISTINCT FKNUM)` для подсчета уникальных фактур, `COUNT(DISTINCT KUNNR)` для уникальных клиентов.\n"
        "4.  Даты в `WHERE` указывай ЯВНО в формате 'YYYYMMDD' (например, `WHERE FKDAT = '20231027'`).\n"
        "5.  ОБЯЗАТЕЛЬНО включай фильтр по дате (`FKDAT`). Нельзя запрашивать данные за все время. Если в инструкции период не конкретизирован (например, 'в прошлом месяце'), рассчитай даты сам.\n"
        "6.  Для фильтрации по дивизиону используй поля `ZZDVAN`, `ZZDVAN2`, ..., `ZZDVAN5`. Если указан код дивизиона (например, '100'), используй его в `WHERE` (например, `WHERE ZZDVAN = '100'`). Если указано название (например, 'Урал'), найди соответствующий код в справочнике дивизионов и используй его.\n"
        "7.  При делении (например, для расчета средней цены или наценки) ИСПОЛЬЗУЙ `CASE WHEN <знаменатель> != 0 THEN <числитель> / <знаменатель> ELSE 0 END` для избежания деления на ноль.\n"
        "8.  Формула наценки: `CASE WHEN ZZACOST != 0 THEN (ZZMARG / ZZACOST) * 100 ELSE 0 END`.\n"
        "9.  ЗАПРЕЩЕНО: `SELECT *`, `WITH` (CTE), `NULLIF`, подзапросы (старайся избегать, если возможно).\n"
        "10. Используй псевдонимы для таблицы (например, `T1`) и для вычисляемых полей (`AS alias_name`).\n"
        "11. Если не указано иное, предполагай, что нужна сумма (`SUM`) для стоимостных показателей и количества.\n"
        "12. Если не указано количество записей для вывода, добавь `LIMIT 20`.\n\n"
        "ЗАДАЧА: На основе инструкции пользователя, напиши ОДИН SQL-запрос.\n"
        "ОТВЕТ ДОЛЖЕН СОДЕРЖАТЬ ТОЛЬКО SQL-КОД, без каких-либо пояснений ДО или ПОСЛЕ."
    )

    conversation = [
        SystemMessage(content=sys_msg_content),
        HumanMessage(content=f"Инструкция пользователя: {validated_instruction}")
    ]

    print("Вызов LLM для генерации SQL...")
    try:
        response = llm.invoke(conversation)
        sql_query = response.content.strip()
        # Простая очистка от возможных ```sql ``` блоков
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()

        print(f"Сгенерирован SQL: \n{sql_query}")
        # Возвращаем SQL как сообщение агента
        return {"messages": [AIMessage(content=sql_query)]}
    except Exception as e:
        print(f"Ошибка при вызове LLM в generate_sql_query: {e}")
        return {"messages": [AIMessage(content=f"Произошла ошибка при генерации SQL: {e}")]}


def comment_sql_query(state: MessagesState) -> Dict[str, Union[List[BaseMessage], str, None]]:
    """
    Генерирует комментарий к SQL-запросу для пользователя.
    """
    print(f"\n--- Узел: comment_sql_query ---")
    current_messages = state['messages']
    final_instruction = state.get('final_instruction') # Берем сохраненную инструкцию

    # Последнее сообщение - это должен быть SQL-запрос или сообщение об ошибке/пропуске
    last_message = current_messages[-1]
    sql_query = ""
    if isinstance(last_message, AIMessage) and \
       "sql generation skipped" not in last_message.content.lower() and \
       "ошибка при генерации sql" not in last_message.content.lower():
        sql_query = last_message.content.strip()
    else:
        print("SQL-запрос не найден или была ошибка на предыдущем шаге. Комментарий не будет сгенерирован.")
        # Возвращаем последнее сообщение (ошибку или пропуск) как финальный ответ
        return {"messages": [last_message]}

    if not final_instruction:
        print("Предупреждение: Исходная инструкция не найдена в состоянии. Комментарий может быть неполным.")
        # Попробуем найти исходный запрос пользователя как fallback
        for msg in reversed(current_messages[:-1]):
            if isinstance(msg, HumanMessage):
                final_instruction = msg.content # Используем последний запрос пользователя
                break
        if not final_instruction:
            final_instruction = "[Инструкция не найдена]"


    print(f"SQL для комментирования: \n{sql_query}")
    print(f"На основе инструкции: {final_instruction}")
    
    sys_msg_content =(
        "Ты - ассистент, который объясняет пользователю, что покажет результат выполненного SQL-запроса.\n"
        "Пользователь НЕ ВИДИТ сам SQL-запрос.\n"
        "Твоя задача - на основе ИНСТРУКЦИИ пользователя и сгенерированного SQL-ЗАПРОСА написать ПОНЯТНЫЙ комментарий.\n\n"
        "Правила для комментария:\n"
        "1. Начни с фразы, описывающей, ЧТО будет показано (например, 'Хорошо, я покажу...', 'Результат покажет...', 'Вот данные о...').\n"
        "2. Перечисли ПОЛЯ, которые выводятся в `SELECT` части SQL-запроса. Используй понятные названия или псевдонимы из SQL (например, '...общую чистую стоимость (total_net_value) и количество фактур (invoice_count)...'). НЕ используй `<placeholder>`.\n"
        "3. Укажи КЛЮЧЕВЫЕ ФИЛЬТРЫ из `WHERE` части SQL: период дат (например, '...за период с 2023-10-01 по 2023-10-31'), дивизион (например, '...для дивизиона 'Урал' (код 200)'), и другие важные условия.\n"
        "4. Если использовались ФОРМУЛЫ (например, расчет наценки), кратко упомяни это (например, '...также будет рассчитана наценка').\n"
        "5. Если есть `GROUP BY`, укажи, по каким полям сгруппированы данные (например, '...с группировкой по материалам').\n"
        "6. Если есть `LIMIT`, упомяни ограничение (например, '...будут показаны первые 20 записей').\n"
        "7. Говори в настоящем или будущем времени ('Запрос покажет...', 'Вы увидите...').\n"
        "8. Будь краток, понятен и дружелюбен. Не используй технический жаргон, кроме названий полей из SELECT.\n"
        "9. НЕ включай сам SQL-запрос в ответ.\n\n"
        "ЗАДАЧА: Сформируй итоговый комментарий для пользователя."
    )

    human_msg_content = (
        f"Инструкция пользователя (на основе которой генерировался SQL):\n{final_instruction}\n\n"
        f"Сгенерированный SQL:\n{sql_query}\n\n"
        "Напиши комментарий для пользователя согласно правилам."
    )

    conversation = [
        SystemMessage(content=sys_msg_content),
        HumanMessage(content=human_msg_content)
    ]

    print("Вызов LLM для генерации комментария...")
    try:
        response = llm.invoke(conversation)
        comment = response.content.strip()
        print(f"Сгенерирован комментарий: {comment}")

        # Мы хотим, чтобы финальным сообщением был именно комментарий
        # SQL-запрос уже есть в истории сообщений state['messages'][-1]
        # Поэтому возвращаем только комментарий
        return {"messages": [AIMessage(content=comment)]}
    except Exception as e:
        print(f"Ошибка при вызове LLM в comment_sql_query: {e}")
        # Возвращаем сообщение об ошибке вместо комментария
        return {"messages": [AIMessage(content=f"Произошла ошибка при генерации комментария: {e}")]}


# --- Сборка графа ---
workflow = StateGraph(MessagesState)

# Добавляем узлы
workflow.add_node("validate_instruction", validate_instruction)
workflow.add_node("generate_sql_query", generate_sql_query)
workflow.add_node("comment_sql_query", comment_sql_query)

# Определяем точку входа
workflow.set_entry_point("validate_instruction")

# Определяем переходы
workflow.add_edge("validate_instruction", "generate_sql_query")
workflow.add_edge("generate_sql_query", "comment_sql_query")

# Определяем конечную точку
workflow.add_edge("comment_sql_query", END)

# --- Компиляция с чекпоинтером ---
# Используем SqliteSaver для сохранения состояния каждого диалога
try:
    memory = SqliteSaver.from_conn_string(CHECKPOINT_DB)
    print(f"Чекпоинтер инициализирован с использованием '{CHECKPOINT_DB}'.")
except Exception as e:
    print(f"Ошибка при инициализации SqliteSaver для '{CHECKPOINT_DB}': {e}")
    print("Убедитесь, что у вас есть права на запись в текущей директории.")
    exit(1)

# Компилируем граф, подключая чекпоинтер
final_agent = workflow.compile(checkpointer=memory)
print("Граф успешно скомпилирован.")

# --- Функция для запуска диалога ---

def run_conversation(user_id: str, initial_message: str):
    """Запускает или продолжает диалог для указанного user_id."""
    thread_id = f"user_{user_id}" # Уникальный ID для диалога пользователя
    config = {"configurable": {"thread_id": thread_id}}

    print(f"\n===== Диалог для {thread_id} =====")
    print(f"Пользователь: {initial_message}")

    # Получаем текущее состояние (историю) для этого пользователя
    current_state = None
    try:
        current_state = final_agent.get_state(config)
    except Exception as e:
        print(f"Предупреждение: Не удалось получить состояние для {thread_id}. Возможно, база данных чекпоинтов повреждена или недоступна. Ошибка: {e}")

    is_new_thread = not current_state or not current_state.values.get('messages')

    # Формируем входное сообщение
    input_message = HumanMessage(content=initial_message)
    # Начинаем с пустого состояния для этого вызова,
    # чекпоинтер сам подгрузит историю и добавит базовую + новую
    input_data = {"messages": []}
    final_instruction_for_input = None # Не передаем инструкцию на вход

    if is_new_thread:
        print("Новый диалог, добавляем базовую историю.")
        if base_history_messages:
            # Добавляем базовую историю ПЕРЕД первым сообщением пользователя
            input_data["messages"].extend(base_history_messages)
        else:
            print("Базовая история пуста или не загружена.")

    # Добавляем текущее сообщение пользователя
    input_data["messages"].append(input_message)

    # Запускаем граф
    print("\nЗапуск графа...")
    final_result_message = None
    try:
        # Используем invoke для получения только конечного результата
        final_state = final_agent.invoke(input_data, config=config)

        # Извлекаем последнее сообщение из итогового состояния
        if final_state and final_state.get('messages'):
            final_result_message = final_state['messages'][-1]
        else:
             print("Предупреждение: Итоговое состояние не содержит сообщений.")

    except Exception as e:
        print(f"\n!!! Ошибка во время выполнения графа для {thread_id}: {e}")
        import traceback
        traceback.print_exc() # Печатаем traceback для отладки

    print("\n--- Итоговый ответ ---")
    if final_result_message and isinstance(final_result_message, AIMessage):
         print(f"Агент: {final_result_message.content}")
    elif final_result_message:
         print(f"Агент (неожиданный тип ответа: {type(final_result_message)}): {final_result_message}")
    else:
        print(f"Агент: [Нет ответа или произошла ошибка выполнения графа]")

    print(f"===== Конец диалога для {thread_id} =====")
    return final_state # Возвращаем состояние для возможного анализа

# --- Основной блок выполнения ---
if __name__ == "__main__":

    # Убедимся, что директории существуют
    os.makedirs("gpt_db/data/confs", exist_ok=True)
    # Создадим пустые файлы, если их нет, чтобы избежать ошибок при первом запуске
    for filepath in [CONFIG_FILE, STRUCTURE_FILE, DIVISIONS_FILE, BASE_HISTORY_FILE]:
        if not os.path.exists(filepath):
            print(f"Предупреждение: Файл '{filepath}' не найден. Создается пустой файл.")
            try:
                # Для конфига создадим базовую структуру
                if filepath == CONFIG_FILE:
                     with open(filepath, 'w', encoding='utf-8') as f:
                         yaml.dump({"GIGACHAT_CREDENTIALS": "YOUR_GIGACHAT_API_KEY_HERE"}, f)
                     print(f"!!! ВНИМАНИЕ: Пожалуйста, отредактируйте '{CONFIG_FILE}' и вставьте ваш реальный GIGACHAT_CREDENTIALS.")
                else:
                    with open(filepath, 'w', encoding='utf-8') as f:
                        pass # Просто создаем пустой файл
            except Exception as e:
                print(f"Не удалось создать файл '{filepath}': {e}")
                # Не выходим, т.к. данные могли быть загружены ранее, но предупреждаем

    # Перезагружаем данные на случай, если файлы только что создались
    GIGACHAT_CREDENTIALS, otgruzki_structure, divisions = _load_config_and_data()
    # Перезагружаем базовую историю
    base_history_messages = load_base_history(BASE_HISTORY_FILE)


    # Пример запросов
    user_querys = [
        "Покажи отгрузки за сегодня", # Должен спросить, ЧТО показать
        "Покажи чистую стоимость и количество фактур за сегодня по всем дивизионам", # Более конкретный
        "Сколько в прошлом месяце отгрузили на Урале", # Должен спросить ЧТО (клиентов, стоимость, кол-во?)
        "Сколько клиентов отгрузилось на дальнем востоке в прошлом году", # Конкретный запрос
        "Кто из них грузился чаще?", # Запрос, зависящий от контекста предыдущего (сложно для текущей реализации без явной передачи контекста между запросами)
        "Покажи топ 5 материалов по сумме маржи за текущий квартал в дивизионе 100" # Сложный запрос
    ]

    # Запускаем диалоги для одного пользователя последовательно
    user_id_for_test = "test_user_001"
    print("\n" + "="*30 + f"\nНачало серии запросов для {user_id_for_test}\n" + "="*30)

    for i, query in enumerate(user_querys):
        print(f"\n--- Запрос {i+1} ---")
        run_conversation(user_id=user_id_for_test, initial_message=query)
        print("-" * 20) # Разделитель между запросами

    print("\n" + "="*30 + f"\nСерия запросов для {user_id_for_test} завершена\n" + "="*30)

    # Можно запустить для другого пользователя, чтобы проверить изоляцию истории
    # print("\n" + "="*30 + f"\nНачало серии запросов для user_002\n" + "="*30)
    # run_conversation(user_id="user_002", initial_message="Привет! Какая структура таблицы?")
    # print("\n" + "="*30 + f"\nСерия запросов для user_002 завершена\n" + "="*30)