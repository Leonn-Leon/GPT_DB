import os
import json
import datetime
import yaml
import sqlite3 # <--- Убедитесь, что импортирован
import traceback
from typing import List, Annotated, Union, Dict, Optional
from typing_extensions import TypedDict

# --- Langchain & Langgraph Imports ---
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph.message import add_messages

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, BaseMessage
from langchain_gigachat.chat_models import GigaChat

# --- SQLGlot Imports ---
try:
    from sqlglot import parse_one, condition
    from sqlglot.errors import ParseError
    SQLGLOT_AVAILABLE = True
except ImportError:
    print("Предупреждение: Библиотека sqlglot не найдена. Функционал ограничений SQL будет недоступен.")
    print("Установите ее: pip install sqlglot")
    SQLGLOT_AVAILABLE = False
    # Определим заглушки, чтобы код не падал при импорте
    class ParseError(Exception): pass
    def parse_one(*args, **kwargs): raise NotImplementedError("sqlglot не установлен")
    def condition(*args, **kwargs): raise NotImplementedError("sqlglot не установлен")

# --- Определение состояния графа ---
class MessagesState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    final_instruction: Optional[str]
    user_id: Optional[str]
    report_id: Optional[str]
    restrictions_applied: bool


class GPTAgent:
    """
    Класс GPT-агента для генерации SQL-запросов к БД отгрузок на основе диалога,
    с применением ограничений доступа.
    Использует явную инициализацию SqliteSaver.
    """
    def __init__(self,
                 config_file: str = "gpt_db/data/confs/config.yaml",
                 structure_file: str = 'gpt_db/data/confs/otgruzki_structure.txt',
                 divisions_file: str = 'gpt_db/data/confs/divisions.txt',
                 base_history_file: str = "history_base.json",
                 checkpoint_db: str = "checkpoints.sqlite",
                 authority_db_path: str = 'data/authority.db',
                 llm_model: str = "GigaChat-2-Max",
                 llm_temperature: float = 0.01,
                 llm_timeout: int = 600):

        # --- Присваивание атрибутов ---
        self.config_file = config_file
        self.structure_file = structure_file
        self.divisions_file = divisions_file
        self.base_history_file = base_history_file
        self.checkpoint_db = checkpoint_db
        self.authority_db_path = authority_db_path
        self.llm_model = llm_model
        self.llm_temperature = llm_temperature
        self.llm_timeout = llm_timeout
        self.memory = None # Инициализируем чекпоинтер как None
        self._sqlite_conn = None # Для хранения соединения с БД чекпоинтов

        # --- Проверки доступности ---
        if not SQLGLOT_AVAILABLE:
            print("ВНИМАНИЕ: sqlglot не доступен, узел применения ограничений будет пропускаться.")
        if not os.path.exists(self.authority_db_path):
             print(f"Предупреждение: Файл БД авторизации '{self.authority_db_path}' не найден.")

        # --- Загрузка конфигурации и данных ---
        self.gigachat_credentials, self.db_schema, self.divisions = self._load_config_and_data()

        # --- Инициализация LLM ---
        self.llm = self._initialize_llm()

        # --- Загрузка базовой истории ---
        print(f"Загрузка базовой истории из {self.base_history_file}...")
        self.base_history_messages = self._load_base_history()
        print(f"Загружено {len(self.base_history_messages)} сообщений базовой истории.")

        # --- Настройка чекпоинтера (ЯВНАЯ ИНИЦИАЛИЗАЦИЯ) ---
        try:
            # 1. Создаем соединение SQLite явно
            # Возвращаем check_same_thread=False, т.к. langgraph может использовать потоки
            self._sqlite_conn = sqlite3.connect(self.checkpoint_db, check_same_thread=False)
            print(f"SQLite соединение к '{self.checkpoint_db}' создано (check_same_thread=False).")

            # 2. Передаем объект соединения в конструктор SqliteSaver
            self.memory = SqliteSaver(conn=self._sqlite_conn)
            print(f"Чекпоинтер SqliteSaver инициализирован с явным соединением.")

            # Проверка типа для уверенности
            if not isinstance(self.memory, SqliteSaver):
                 raise TypeError(f"Ошибка: Инициализация вернула {type(self.memory)}, ожидался SqliteSaver.")
            print(f"Тип self.memory: {type(self.memory)}")

        except TypeError as te:
             print(f"Ошибка типа при инициализации SqliteSaver: {te}")
             self.close_connection() # Закрываем соединение при ошибке
             raise
        except Exception as e:
            print(f"Ошибка при инициализации SqliteSaver для '{self.checkpoint_db}': {e}")
            self.close_connection() # Закрываем соединение при ошибке
            raise

        # --- Сборка и компиляция графа ---
        try:
            self.graph = self._build_graph()
            # Передаем созданный self.memory в compile
            self.compiled_agent = self.graph.compile(checkpointer=self.memory)
            print("Граф успешно скомпилирован.")
        except Exception as e:
             print(f"Ошибка при компиляции графа: {e}")
             self.close_connection() # Закрываем соединение при ошибке
             raise

    def close_connection(self):
        """Закрывает соединение с БД чекпоинтера, если оно было открыто."""
        if self._sqlite_conn:
            try:
                print(f"Закрытие соединения с БД чекпоинтера '{self.checkpoint_db}'...")
                self._sqlite_conn.close()
                self._sqlite_conn = None
                print("Соединение закрыто.")
            except Exception as e:
                print(f"Ошибка при закрытии соединения с БД чекпоинтера: {e}")

    # --- Методы загрузки и инициализации (без изменений) ---
    def _load_config_and_data(self) -> tuple[str, str, str]:
        # ... (код как в вашем скрипте) ...
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            gigachat_credentials = config["GIGACHAT_CREDENTIALS"]
        except Exception as e:
            print(f"Ошибка при чтении файла конфигурации '{self.config_file}': {e}")
            raise

        try:
            with open(self.structure_file, 'r', encoding='utf-8') as file:
                db_schema = file.read().strip()
        except Exception as e:
            print(f"Ошибка при чтении файла схемы '{self.structure_file}': {e}")
            raise

        try:
            with open(self.divisions_file, 'r', encoding='utf-8') as file:
                divisions_data = file.read().strip()
        except Exception as e:
            print(f"Ошибка при чтении файла дивизионов '{self.divisions_file}': {e}")
            raise

        return gigachat_credentials, db_schema, divisions_data


    def _initialize_llm(self) -> GigaChat:
        try:
            llm = GigaChat(
                credentials=self.gigachat_credentials,
                model=self.llm_model,
                verify_ssl_certs=False,
                temperature=self.llm_temperature,
                timeout=self.llm_timeout
            )
            print(f"GigaChat LLM ({self.llm_model}) инициализирован успешно.")
            return llm
        except Exception as e:
            print(f"Ошибка при инициализации GigaChat: {e}")
            print("Убедитесь, что GIGACHAT_CREDENTIALS верны и модель доступна.")
            raise

    def _load_base_history(self) -> list[BaseMessage]:
        messages = []
        if not os.path.exists(self.base_history_file):
            print(f"Предупреждение: Файл базовой истории '{self.base_history_file}' не найден. Будет создан пустой.")
            try:
                with open(self.base_history_file, 'w', encoding='utf-8') as f: pass
            except Exception as e:
                 print(f"Не удалось создать файл базовой истории '{self.base_history_file}': {e}")
            return messages
        try:
            with open(self.base_history_file, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    line = line.strip()
                    if not line: continue
                    try:
                        record = json.loads(line)
                        msg_type = record.get("type")
                        content = record.get("content", "")
                        if msg_type in ["human_answer", "human"]: messages.append(HumanMessage(content=content))
                        elif msg_type in ["agent_answer", "ai"]: messages.append(AIMessage(content=content))
                        elif msg_type == "system": messages.append(SystemMessage(content=content))
                        else: print(f"Предупреждение: Неизвестный тип сообщения '{msg_type}' в строке {i+1} файла '{self.base_history_file}'.")
                    except json.JSONDecodeError as e: print(f"Ошибка декодирования JSON в строке {i+1} файла '{self.base_history_file}': {e}")
                    except Exception as e: print(f"Ошибка обработки строки {i+1} файла '{self.base_history_file}': {e}")
        except Exception as e: print(f"Неожиданная ошибка при загрузке базовой истории '{self.base_history_file}': {e}")
        return messages


    def validate_instruction(self, state: MessagesState) -> Dict[str, Union[List[BaseMessage], str, None]]:
        current_messages = state['messages']
        # Инициализируем final_instruction как None в начале
        output_state = {"messages": [], "final_instruction": None}

        # Если последнее сообщение не HumanMessage, что-то пошло не так (или это начало)
        if not current_messages or not isinstance(current_messages[-1], HumanMessage):
             print("Предупреждение: validate_instruction вызван без начального сообщения пользователя.")
             # Можно вернуть текущие сообщения или специальное сообщение об ошибке
             output_state["messages"] = current_messages
             return output_state # Возвращаем как есть

        last_user_message = current_messages[-1].content.strip()

        print(f"\n--- Узел: validate_instruction ---")
        print(f"Получено сообщение: {last_user_message}")

        sys_msg_content = (
            f"Описание структуры БД:\n{self.db_schema}\n\n"
            f"Справочник дивизионов:\n{self.divisions}\n"
            f"Сегодняшняя дата: {datetime.date.today().strftime('%Y%m%d')}\n\n"
            "Ты ассистент, помогающий сформулировать точный запрос к базе данных отгрузок. Твоя задача - валидировать и уточнять запрос пользователя.\n"
            "Правила валидации:\n"
            "1. Проверь, соответствует ли запрос доступным полям в структуре БД. Если пользователь упоминает несуществующие поля, укажи на это.\n"
            "2. Убедись, что из запроса ЯСНО, какие КОНКРЕТНЫЕ ПОЛЯ (столбцы) нужно вывести. Запрос 'покажи отгрузки' невалиден - нужно уточнить, ЧТО именно показать (например, 'покажи чистую стоимость и количество'). Запрещено выводить все поля (`SELECT *`).\n"
            "3. Проверь, понятны ли фильтры (даты, дивизионы, клиенты и т.д.). Если период не указан, уточни (нельзя запрашивать данные за все время).\n"
            "4. Если что-то неясно или не соответствует правилам, задай КОРОТКИЙ уточняющий вопрос. Предлагай варианты, если это уместно (например, 'Уточните, какие поля вывести: количество, стоимость или оба?').\n"
            "5. Если запрос пользователя ПОЛНОСТЬЮ ясен, точен и соответствует правилам, ответь СТРОГО в формате:\n"
            "ok\n<Здесь четко сформулированная итоговая инструкция для генерации SQL>\n"
            f"Пример ответа 'ok':\nok\nПокажи сумму чистой стоимости (NETWR) и количество фактур (FKIMG) для дивизиона '100' за вчерашний день ({datetime.date.today() - datetime.timedelta(days=1):%Y%m%d}).\n"
            "ВАЖНО: Поле даты ФАКТУРЫ (FKDAT) в таблице ЕСТЬ. Не говори, что его нет.\n"
            "Отвечай кратко: либо уточняющий вопрос, либо 'ok' с итоговой инструкцией."
            "Не пиши сам SQL запрос!!!"
        )

        # Используем только текущую историю для LLM в этом узле
        conversation_for_llm = [SystemMessage(content=sys_msg_content)] + current_messages

        # !!! ИНТЕРАКТИВНЫЙ ЦИКЛ !!!
        while True:
            print("\nВызов LLM для валидации...")
            try:
                response = self.llm.invoke(conversation_for_llm)
                result_text = response.content.strip()
                print(f"Ответ LLM (валидация): {result_text}")
            except Exception as e:
                print(f"Ошибка при вызове LLM в validate_instruction: {e}")
                output_state["messages"] = [AIMessage(content=f"Произошла ошибка при обращении к языковой модели: {e}")]
                return output_state # Возвращаем ошибку

            lower_text = result_text.lower()
            if lower_text.startswith("ok"):
                parts = result_text.split('\n', 1)
                final_instruction = parts[1].strip() if len(parts) > 1 else "Инструкция не извлечена после 'ok'"
                print(f"✅ Инструкция принята: {final_instruction}")
                # Возвращаем сообщение "ok..." и сохраняем инструкцию в state
                output_state["messages"] = [AIMessage(content=result_text)]
                output_state["final_instruction"] = final_instruction
                return output_state
            else:
                # Модель просит уточнений
                print(f"⚠️ Уточнение от модели: {result_text}")
                try:
                    ai_question_message = AIMessage(content=result_text)
                    # Добавляем вопрос в локальную историю для следующей итерации
                    conversation_for_llm.append(ai_question_message)

                    # !!! БЛОКИРУЮЩИЙ ВВОД !!!
                    clarification = input(f"🔄 [{datetime.datetime.now().strftime('%H:%M:%S')}] Введите уточнение (или 'stop' для выхода):\n{result_text}\n> ")
                    clarification = clarification.strip()

                    user_clarification_message = HumanMessage(content=clarification)

                    if clarification.lower() == 'stop' or not clarification:
                        print("Прервано пользователем.")
                        # Возвращаем сообщения диалога отмены
                        output_state["messages"] = [ai_question_message, user_clarification_message, AIMessage(content="Операция отменена пользователем.")]
                        output_state["final_instruction"] = None # Сбрасываем инструкцию
                        return output_state

                    # Добавляем ответ пользователя в локальную историю для следующей итерации
                    conversation_for_llm.append(user_clarification_message)
                    # ВАЖНО: Эти сообщения (ai_question, user_clarification) не добавляются
                    # в глобальное состояние state['messages'] до завершения цикла (ok/stop).
                    # Они будут потеряны в чекпоинте, если процесс прервется здесь.

                except EOFError:
                     print("Ошибка ввода (EOF), прерывание.")
                     output_state["messages"] = [AIMessage(content="Произошла ошибка ввода, операция прервана.")]
                     output_state["final_instruction"] = None
                     return output_state
                except Exception as e:
                     print(f"Неожиданная ошибка ввода: {e}")
                     output_state["messages"] = [AIMessage(content=f"Произошла ошибка обработки ввода: {e}")]
                     output_state["final_instruction"] = None
                     return output_state

    def generate_sql_query(self, state: MessagesState) -> Dict[str, Union[List[BaseMessage], str, None]]:
        # ... (код без изменений, но теперь возвращает только messages) ...
        print(f"\n--- Узел: generate_sql_query ---")
        validated_instruction = state.get('final_instruction')
        # Инициализируем флаг ограничений как False
        output_state = {"messages": [], "restrictions_applied": False}

        if not validated_instruction:
             print("Ошибка: Валидированная инструкция отсутствует в состоянии.")
             output_state["messages"] = [AIMessage(content="-- SQL generation skipped (no instruction) --")]
             return output_state

        print(f"Инструкция для генерации SQL: {validated_instruction}")

        sys_msg_content = (
            "Ты – эксперт по SQL (HANA) и анализу данных. Твоя задача - написать ТОЧНЫЙ и ОПТИМАЛЬНЫЙ SQL-запрос к базе данных SAP HANA.\n"
            f"ЗАПРОСЫ ИДУТ ТОЛЬКО К ТАБЛИЦЕ: SAPABAP1.ZZSDM_117_CUS\n"
            f"Её структура:\n{self.db_schema}\n\n"
            f"Справочник дивизионов (используй коды в запросе):\n{self.divisions}\n\n"
            f"Сегодняшняя дата: {datetime.date.today().strftime('%Y%m%d')}\n\n"
            "СТРОГИЕ ПРАВИЛА ГЕНЕРАЦИИ SQL:\n"
            "1. Используй ТОЛЬКО поля из предоставленной структуры таблицы `SAPABAP1.ZZSDM_117_CUS`.\n"
            "2. Для полей-характеристик (текстовые, даты, коды) используй `GROUP BY`.\n"
            "3. Для полей-показателей (числовые: NETWR, FKIMG, ZZACOST, ZZMARG) используй агрегатные функции (`SUM`, `COUNT`, `AVG`). `COUNT(DISTINCT FKNUM)` для подсчета уникальных фактур, `COUNT(DISTINCT KUNNR)` для уникальных клиентов.\n"
            "4. Даты в `WHERE` указывай ЯВНО в формате 'YYYYMMDD' (например, `WHERE FKDAT = '20231027'`).\n"
            "5. ОБЯЗАТЕЛЬНО включай фильтр по дате (`FKDAT`). Нельзя запрашивать данные за все время. Если в инструкции период не конкретизирован (например, 'в прошлом месяце'), рассчитай даты сам.\n"
            "6. Для фильтрации по дивизиону используй поля `ZZDVAN`, `ZZDVAN2`, ..., `ZZDVAN5`. Если указан код дивизиона (например, '100'), используй его в `WHERE` (например, `WHERE ZZDVAN = '100'`). Если указано название (например, 'Урал'), найди соответствующий код в справочнике дивизионов и используй его.\n"
            "7. При делении (например, для расчета средней цены или наценки) ИСПОЛЬЗУЙ `CASE WHEN <знаменатель> != 0 THEN <числитель> / <знаменатель> ELSE 0 END` для избежания деления на ноль.\n"
            "8. Формула наценки: `CASE WHEN ZZACOST != 0 THEN (ZZMARG / ZZACOST) * 100 ELSE 0 END`.\n"
            "9. ЗАПРЕЩЕНО: `SELECT *`, `WITH` (CTE), `NULLIF`, подзапросы (старайся избегать, если возможно).\n"
            "10. Используй псевдонимы для таблицы (например, `T1`) и для вычисляемых полей (`AS alias_name`).\n"
            "11. Если не указано иное, предполагай, что нужна сумма (`SUM`) для стоимостных показателей и количества.\n"
            "12. Если не указано количество записей для вывода, добавь `LIMIT 20`.\n\n"
            "ЗАДАЧА: На основе инструкции пользователя, напиши ОДИН SQL-запрос.\n"
            "ОТВЕТ ДОЛЖЕН СОДЕРЖАТЬ ТОЛЬКО SQL-КОД, без каких-либо пояснений ДО или ПОСЛЕ.\n"
        )

        conversation = [
            SystemMessage(content=sys_msg_content),
            HumanMessage(content=f"Инструкция пользователя: {validated_instruction}")
        ]

        print("Вызов LLM для генерации SQL...")
        try:
            response = self.llm.invoke(conversation)
            sql_query = response.content.strip()
            if sql_query.startswith("```sql"): sql_query = sql_query[6:]
            if sql_query.endswith("```"): sql_query = sql_query[:-3]
            sql_query = sql_query.strip()

            print(f"Сгенерирован SQL: \n{sql_query}")
            output_state["messages"] = [AIMessage(content=sql_query)]
        except Exception as e:
            print(f"Ошибка при вызове LLM в generate_sql_query: {e}")
            output_state["messages"] = [AIMessage(content=f"Произошла ошибка при генерации SQL: {e}")]

        return output_state # Возвращаем только сообщения и флаг по умолчанию

    # --- Новый узел и его логика для применения ограничений ---
    def _apply_sql_restrictions_logic(self, sql_query: str, user: str, report: str) -> tuple[str, bool]:
        """
        Применяет ограничения доступа к SQL-запросу.
        Возвращает кортеж: (модифицированный_sql, были_ли_применены_ограничения_успешно).
        """
        zvobj = ''
        auth = ''
        restricted_sql = sql_query # По умолчанию возвращаем исходный запрос
        restrictions_applied_successfully = False # Флаг успеха

        if not SQLGLOT_AVAILABLE:
            print("Пропуск применения ограничений: sqlglot не установлен.")
            return restricted_sql, restrictions_applied_successfully

        if not user or not report:
            print("Предупреждение: user_id или report_id не предоставлены для применения ограничений.")
            return sql_query, restrictions_applied_successfully

        if not os.path.exists(self.authority_db_path):
             print(f"Ошибка: Файл БД авторизации '{self.authority_db_path}' не найден. Ограничения не применены.")
             return sql_query, restrictions_applied_successfully

        try:
            connection = sqlite3.connect(self.authority_db_path)
            connection.row_factory = sqlite3.Row
            cursor = connection.cursor()

            # Запрос для поиска наиболее специфичного правила
            # Используем стандартный LIKE с '%'
            # Сортируем по длине zvobj DESC, чтобы самое специфичное правило было первым
            # Добавляем условие WHERE zuser = ? для безопасности
            cursor.execute(
                """
                SELECT zvobj, auth
                FROM ZARM_AUTH_CFO
                WHERE zuser = ? AND (
                    zvobj = ?
                    OR (? LIKE zvobj || '%' AND zvobj LIKE '7%') -- Обрабатываем '7*' как '7%'
                    OR zvobj = '*'
                )
                ORDER BY LENGTH(zvobj) DESC
                LIMIT 1
                """,
                (user, report, report) # Передаем user и report дважды для плейсхолдеров
            )
            best_match = cursor.fetchone()
            connection.close()

            if best_match:
                auth = best_match['auth']
                print(f"Найдены права для user='{user}', report='{report}': auth='{auth}' (zvobj='{best_match['zvobj']}')")
            else:
                 print(f"Права для user='{user}', report='{report}' не найдены. Применяется запрет (1=2).")
                 auth = '1 = 2' # Если прав нет совсем

            # Если auth пустой или null из БД, тоже применяем запрет
            if not auth:
                print(f"Строка прав найдена, но поле 'auth' пустое или NULL. Применяется запрет (1=2).")
                auth = '1 = 2'

            # --- Применение ограничения с помощью sqlglot ---
            try:
                # !!! ВАЖНО: Валидация строки auth перед использованием !!!
                # Простая проверка: не содержит ли точка с запятой (примитивная защита от инъекций)
                if ';' in auth:
                    print(f"ОШИБКА БЕЗОПАСНОСТИ: Строка прав '{auth}' содержит ';'. Ограничения не применены.")
                    # Можно вернуть исходный SQL или специальную ошибку
                    return sql_query, restrictions_applied_successfully

                # Пытаемся распарсить сам auth как условие WHERE (дополнительная проверка)
                try:
                    # Указываем диалект вашей основной БД (например, 'hana', 'sqlite', 'postgres')
                    # Если не указать, sqlglot попытается угадать
                    parse_one(f"SELECT * FROM dummy WHERE {auth}")
                    print(f"Строка прав '{auth}' успешно распарсена как условие.")
                except ParseError as e_auth:
                    print(f"ОШИБКА ПАРСИНГА ПРАВ: Не удалось распарсить строку прав '{auth}' как валидное SQL условие: {e_auth}")
                    print("Применяется запрет (1=2) из-за невалидных прав.")
                    auth = '1 = 2' # Применяем запрет, если правило некорректно
                except Exception as e_auth_other:
                     print(f"Неожиданная ошибка при парсинге прав '{auth}': {e_auth_other}")
                     print("Применяется запрет (1=2) из-за ошибки.")
                     auth = '1 = 2'

                # Парсим основной SQL-запрос
                parsed = parse_one(sql_query) # Укажите диалект вашей БД (HANA)

                # Создаем условие из (потенциально скорректированной) строки auth
                where_condition = condition(auth)

                # Добавляем условие через AND к существующему WHERE или создаем WHERE
                parsed_with_restriction = parsed.where(where_condition) # copy=False для модификации на месте
                restricted_sql = parsed_with_restriction.sql(pretty=True, identify=True) # Указываем диалект для вывода
                print(f"SQL после применения ограничений:\n{restricted_sql}")
                restrictions_applied_successfully = True # Успешно применили

            except ParseError as e_parse:
                print(f"Ошибка парсинга основного SQL библиотекой sqlglot: {e_parse}")
                print(f"Исходный SQL:\n{sql_query}")
                print("Ограничения не применены к SQL из-за ошибки парсинга.")
                # Возвращаем исходный SQL
            except Exception as e_sqlglot:
                 print(f"Неожиданная ошибка при модификации SQL с помощью sqlglot: {e_sqlglot}")
                 traceback.print_exc()
                 # Возвращаем исходный SQL

        except sqlite3.Error as e_sqlite:
            print(f"Ошибка при доступе к БД авторизации '{self.authority_db_path}': {e_sqlite}")
            print("Ограничения не применены из-за ошибки БД.")
            # Возвращаем исходный SQL
        except Exception as e:
            print(f"Неожиданная ошибка в _apply_sql_restrictions_logic: {e}")
            traceback.print_exc()
            # Возвращаем исходный SQL

        return restricted_sql, restrictions_applied_successfully

    def apply_sql_restrictions(self, state: MessagesState) -> Dict[str, Union[List[BaseMessage], bool]]:
        """Узел графа для применения ограничений к сгенерированному SQL."""
        print(f"\n--- Узел: apply_sql_restrictions ---")
        current_messages = state['messages']
        user_id = state.get('user_id')
        report_id = state.get('report_id')
        # Инициализируем выходное состояние
        output_state = {"messages": current_messages, "restrictions_applied": False}

        if not SQLGLOT_AVAILABLE:
            print("Пропуск узла: sqlglot не установлен.")
            return output_state # Возвращаем состояние без изменений

        # Ищем последний AIMessage, который должен содержать SQL
        sql_query = ""
        sql_message_index = -1
        for i in range(len(current_messages) - 1, -1, -1):
            msg = current_messages[i]
            if isinstance(msg, AIMessage):
                # Проверяем, не является ли это сообщением об ошибке/пропуске/отмене
                content_lower = msg.content.lower()
                if "sql generation skipped" not in content_lower and \
                   "ошибка при генерации sql" not in content_lower and \
                   "операция отменена" not in content_lower:
                    sql_query = msg.content.strip()
                    sql_message_index = i
                    break
                else:
                    # Нашли сообщение об ошибке/пропуске/отмене, дальше искать SQL не нужно
                    print("Предыдущий узел не вернул SQL-запрос. Ограничения не применяются.")
                    return output_state # Просто передаем дальше

        if not sql_query or sql_message_index == -1:
            print("Не найден SQL-запрос в предыдущих сообщениях. Ограничения не применяются.")
            # Это может случиться, если validate_instruction вернул отмену
            return output_state

        if not user_id:
            print("Критическая ошибка: user_id отсутствует в состоянии. Ограничения не могут быть применены.")
            # В идеале user_id должен всегда присутствовать на этом этапе
            # Можно добавить сообщение об ошибке
            error_msg = AIMessage(content="-- Ошибка: Не удалось применить ограничения доступа (отсутствует ID пользователя) --")
            output_state["messages"] = current_messages[:sql_message_index] + [error_msg] + current_messages[sql_message_index+1:]
            return output_state

        if not report_id:
             print("Предупреждение: report_id отсутствует в состоянии. Используется 'default_report'.")
             report_id = "default_report" # Устанавливаем значение по умолчанию

        print(f"Применение ограничений для user='{user_id}', report='{report_id}' к SQL:\n{sql_query}")

        # Вызываем основную логику
        restricted_sql, restrictions_applied = self._apply_sql_restrictions_logic(sql_query, user_id, report_id)

        # Заменяем исходный SQL в списке сообщений на модифицированный
        # или оставляем исходный, если были ошибки
        new_messages = list(current_messages) # Создаем копию списка
        new_messages[sql_message_index] = AIMessage(content=restricted_sql)

        output_state["messages"] = new_messages
        output_state["restrictions_applied"] = restrictions_applied # Сохраняем флаг

        return output_state

    def comment_sql_query(self, state: MessagesState) -> Dict[str, List[BaseMessage]]:
        """Генерирует комментарий к (возможно ограниченному) SQL-запросу."""
        print(f"\n--- Узел: comment_sql_query ---")
        current_messages = state['messages']
        final_instruction = state.get('final_instruction')
        restrictions_applied = state.get('restrictions_applied', False) # Получаем флаг
        output_state = {"messages": []}

        # Ищем последний AIMessage (должен быть SQL или сообщение об ошибке/пропуске)
        last_message = None
        sql_query = ""
        if current_messages and isinstance(current_messages[-1], AIMessage):
             last_message = current_messages[-1]
             content_lower = last_message.content.lower()
             # Проверяем на ошибки/пропуски/отмены из предыдущих шагов
             if "sql generation skipped" not in content_lower and \
                "ошибка при генерации sql" not in content_lower and \
                "ошибка: не удалось применить ограничения" not in content_lower and \
                "операция отменена" not in content_lower:
                 sql_query = last_message.content.strip()
             else:
                 print("SQL-запрос не найден или была ошибка/отмена на предыдущем шаге. Комментарий не будет сгенерирован.")
                 # Возвращаем последнее сообщение (ошибку/пропуск/отмену) как финальный ответ
                 output_state["messages"] = [last_message]
                 return output_state
        else:
             print("Критическая ошибка: Не найдено финальное сообщение AIMessage для комментирования.")
             # Возвращаем текущие сообщения или сообщение об ошибке
             output_state["messages"] = current_messages + [AIMessage(content="-- Ошибка: Не удалось сгенерировать финальный комментарий --")]
             return output_state


        if not final_instruction:
            print("Предупреждение: Исходная инструкция не найдена в состоянии. Комментарий может быть неполным.")
            # Попробуем найти исходный запрос пользователя как fallback
            for msg in reversed(current_messages[:-1]):
                if isinstance(msg, HumanMessage):
                    final_instruction = msg.content # Используем последний запрос пользователя
                    break
            if not final_instruction:
                final_instruction = "[Инструкция не найдена]"


        print(f"SQL для комментирования (после ограничений, если были):\n{sql_query}")
        print(f"На основе инструкции: {final_instruction}")
        print(f"Ограничения применены: {restrictions_applied}")

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
            "9. НЕ включай сам SQL-запрос в ответ.\n"
            "10. Если к запросу были применены ограничения доступа (флаг restrictions_applied=True), добавь в конце фразу типа: '(Результаты показаны с учетом ваших прав доступа.)'\n\n" # <-- Добавлено правило 10
            "ЗАДАЧА: Сформируй итоговый комментарий для пользователя."
        )

        human_msg_content = (
            f"Инструкция пользователя (на основе которой генерировался SQL):\n{final_instruction}\n\n"
            f"Сгенерированный SQL (возможно, с ограничениями):\n{sql_query}\n\n"
            f"Флаг применения ограничений (restrictions_applied): {restrictions_applied}\n\n" # <-- Передаем флаг в промпт
            "Напиши комментарий для пользователя согласно правилам."
        )

        conversation = [
            SystemMessage(content=sys_msg_content),
            HumanMessage(content=human_msg_content)
        ]

        print("Вызов LLM для генерации комментария...")
        try:
            response = self.llm.invoke(conversation)
            comment = response.content.strip()
            print(f"Сгенерирован комментарий: {comment}")

            # Мы хотим, чтобы финальным сообщением был именно комментарий
            # SQL-запрос уже есть в истории сообщений state['messages'][-1]
            # Поэтому возвращаем только комментарий
            output_state["messages"] = [AIMessage(content=comment)]
        except Exception as e:
            print(f"Ошибка при вызове LLM в comment_sql_query: {e}")
            # Возвращаем сообщение об ошибке вместо комментария
            output_state["messages"] = [AIMessage(content=f"Произошла ошибка при генерации комментария: {e}")]

        return output_state


    # --- Сборка графа ---
    def _build_graph(self) -> StateGraph:
        """Собирает граф LangGraph с узлом ограничений."""
        workflow = StateGraph(MessagesState)

        # Добавляем узлы
        workflow.add_node("validate_instruction", self.validate_instruction)
        workflow.add_node("generate_sql_query", self.generate_sql_query)
        workflow.add_node("apply_sql_restrictions", self.apply_sql_restrictions) # <-- Новый узел
        workflow.add_node("comment_sql_query", self.comment_sql_query)

        # Определяем точку входа
        workflow.set_entry_point("validate_instruction")

        # Определяем переходы
        workflow.add_edge("validate_instruction", "generate_sql_query")
        workflow.add_edge("generate_sql_query", "apply_sql_restrictions") # <-- Новый переход
        workflow.add_edge("apply_sql_restrictions", "comment_sql_query") # <-- Измененный переход
        workflow.add_edge("comment_sql_query", END)

        return workflow

    # --- Функция для запуска диалога ---
    def run(self, user_id: str, message: str, report_id: Optional[str] = "default_report") -> Optional[Dict]:
        """
        Запускает или продолжает диалог для указанного user_id,
        передавая user_id и report_id в состояние графа.
        Использует invoke.
        """
        thread_id = f"user_{user_id}_{report_id}"
        config = {"configurable": {"thread_id": thread_id}}

        print(f"\n===== Диалог для {thread_id} (User: {user_id}, Report: {report_id}) =====")
        print(f"Пользователь: {message}")

        # --- Определение, новый ли это поток (для базовой истории) ---
        # Упрощаем: Не делаем get_state здесь, полагаемся на invoke для загрузки состояния
        # Базовую историю добавим, если invoke вернет пустое начальное состояние (или при ошибке)
        is_new_thread = False # Предполагаем, что поток существует, invoke разберется
        # Можно добавить проверку существования файла чекпоинта, но это не гарантирует наличие потока

        input_messages = []
        # Базовую историю добавим *только* если invoke не сможет загрузить существующую
        # (Логика добавления базовой истории перенесена внутрь try-except ниже)

        # Добавляем текущее сообщение пользователя
        input_messages.append(HumanMessage(content=message))

        input_data = {
            "messages": input_messages,
            "user_id": user_id,
            "report_id": report_id
        }

        print("\nЗапуск графа (используя invoke)...")
        final_state_values = None
        final_result_message = None
        try:
            # Выполняем граф и получаем итоговое состояние
            # invoke сам загрузит состояние из чекпоинтера, если оно есть
            final_state_values = self.compiled_agent.invoke(input_data, config=config)

            # Проверяем результат invoke
            if final_state_values and final_state_values.get('messages'):
                final_result_message = final_state_values['messages'][-1]
            else:
                 print("Предупреждение: Итоговое состояние от invoke не содержит сообщений или пустое.")
                 if not final_state_values: final_state_values = {} # Инициализация для избежания ошибок

        except Exception as e:
            print(f"\n!!! Ошибка во время выполнения графа (invoke) для {thread_id}: {e}")
            traceback.print_exc()
            # Если invoke упал, возможно, это был новый поток, попробуем добавить базовую историю
            # (Это предположение, ошибка могла быть и по другой причине)
            if self.base_history_messages:
                 print("Попытка добавить базовую историю после ошибки invoke...")
                 input_data["messages"] = self.base_history_messages + input_data["messages"]
                 # Повторный вызов invoke не рекомендуется здесь, лучше просто вернуть ошибку
            final_state_values = {"messages": [AIMessage(content=f"Ошибка выполнения: {e}")]} # Возвращаем состояние с ошибкой

        print("\n--- Итоговый ответ ---")
        if final_result_message and isinstance(final_result_message, AIMessage):
             print(f"Агент: {final_result_message.content}")
        elif final_result_message:
             print(f"Агент (неожиданный тип ответа: {type(final_result_message)}): {final_result_message}")
        else:
            # Проверяем, есть ли сообщение об ошибке в последнем сообщении состояния
            error_in_state = False
            if final_state_values and final_state_values.get('messages'):
                 # Убедимся, что messages не пустой список
                 if final_state_values['messages']:
                     last_msg = final_state_values['messages'][-1]
                     # Проверяем, что это сообщение и оно содержит текст ошибки
                     if hasattr(last_msg, 'content'):
                         last_msg_content = last_msg.content.lower()
                         if "ошибка" in last_msg_content or "skipped" in last_msg_content or "отменена" in last_msg_content:
                              print(f"Агент: {last_msg.content}")
                              error_in_state = True
            if not error_in_state:
                print(f"Агент: [Нет ответа или произошла ошибка выполнения графа]")


        print(f"===== Конец диалога для {thread_id} =====")
        return final_state_values

    # Добавим деструктор для попытки закрыть соединение при уничтожении объекта
    def __del__(self):
        self.close_connection()

# --- Основной блок выполнения ---
if __name__ == "__main__":

    # --- Пути к файлам ---
    # Используем os.path.join для кроссплатформенности
    DATA_DIR = os.path.join("gpt_db", "data")
    CONF_DIR = os.path.join(DATA_DIR, "confs")
    # Определяем путь к папке с историей (если она отдельная)
    # Если история лежит прямо в DATA_DIR, измените путь
    DIALOG_CASH_DIR = os.path.join(DATA_DIR, "dialogs_cash")

    CONFIG_FILE = os.path.join(CONF_DIR, "config.yaml")
    STRUCTURE_FILE = os.path.join(CONF_DIR, 'otgruzki_structure.txt')
    DIVISIONS_FILE = os.path.join(CONF_DIR, 'divisions.txt')
    # Убедитесь, что путь к файлу истории правильный
    BASE_HISTORY_FILE = os.path.join(DIALOG_CASH_DIR, "history_base.json")
    # BASE_HISTORY_FILE = os.path.join(DATA_DIR, "history_base.json") # Альтернативный путь, если история в data
    AUTHORITY_DB_FILE = os.path.join(DATA_DIR, 'authority.db') # Путь к вашей существующей БД прав
    CHECKPOINT_DB_FILE = os.path.join(DATA_DIR, "checkpoints.sqlite")

    # --- Создание директорий, если их нет ---
    os.makedirs(CONF_DIR, exist_ok=True)
    # Создаем папку для истории, только если она используется и отличается от DATA_DIR
    if DIALOG_CASH_DIR != DATA_DIR:
        os.makedirs(DIALOG_CASH_DIR, exist_ok=True)

    # --- Проверка наличия критически важной БД авторизации ---
    if not os.path.exists(AUTHORITY_DB_FILE):
        print(f"!!! КРИТИЧЕСКАЯ ОШИБКА: Файл БД авторизации '{AUTHORITY_DB_FILE}' не найден.")
        print("Этот файл необходим для работы ограничений доступа.")
        print("Пожалуйста, создайте и заполните его перед запуском скрипта.")
        exit(1) # Выход, так как без БД прав тест не имеет смысла
    else:
        print(f"БД авторизации найдена: '{AUTHORITY_DB_FILE}'")

    # --- Создание базовых файлов конфигурации/данных (если их нет) ---
    required_files_content = {
        CONFIG_FILE: {"GIGACHAT_CREDENTIALS": "YOUR_GIGACHAT_API_KEY_HERE"},
        STRUCTURE_FILE: "# Описание полей таблицы SAPABAP1.ZZSDM_117_CUS\n# FIELD_NAME (TYPE): Description\nFKDAT (DATE): Дата фактуры\nFKIMG (DECIMAL): Количество фактуры\nNETWR (DECIMAL): Чистая стоимость\nKUNNR (VARCHAR): Номер клиента\nZZDVAN (VARCHAR): Код дивизиона 1\nZCFO1 (VARCHAR): Код ЦФО 1\nZDIV (VARCHAR): Код дивизиона (основной?)\n...", # Добавьте ZCFO1 и ZDIV, если они есть
        DIVISIONS_FILE: "# Код: Название\n01: Див 1\n02: Урал\n03: Див 3\n04: Див 4\n100: Центр\n...", # Добавьте коды 01, 02, 03, 04 если они используются в правах
    }
    for filepath, content in required_files_content.items():
        if not os.path.exists(filepath):
            print(f"Предупреждение: Файл '{filepath}' не найден. Создается базовый файл.")
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    if isinstance(content, dict): # YAML
                        yaml.dump(content, f)
                        if "GIGACHAT_CREDENTIALS" in content:
                             print(f"!!! ВНИМАНИЕ: Пожалуйста, отредактируйте '{filepath}' и вставьте ваш реальный GIGACHAT_CREDENTIALS.")
                    elif isinstance(content, str): # TXT
                        f.write(content)
            except Exception as e:
                print(f"Не удалось создать файл '{filepath}': {e}")
                if "config" in filepath or "structure" in filepath or "divisions" in filepath:
                    exit(1)

    # --- Создание файла базовой истории (если нет) ---
    if not os.path.exists(BASE_HISTORY_FILE):
         print(f"Предупреждение: Файл базовой истории '{BASE_HISTORY_FILE}' не найден. Создается пустой файл.")
         try:
             with open(BASE_HISTORY_FILE, 'w', encoding='utf-8') as f:
                 pass # Создаем пустой файл
         except Exception as e:
             print(f"Не удалось создать файл базовой истории '{BASE_HISTORY_FILE}': {e}")

    # --- Основной запуск агента ---
    agent = None # Инициализируем agent как None
    try:
        # Инициализируем агент, передавая пути к файлам
        agent = GPTAgent(
            config_file=CONFIG_FILE,
            structure_file=STRUCTURE_FILE,
            divisions_file=DIVISIONS_FILE,
            base_history_file=BASE_HISTORY_FILE,
            authority_db_path=AUTHORITY_DB_FILE, # Используем существующую БД
            checkpoint_db=CHECKPOINT_DB_FILE
        )

        # --- Тестовые запросы для проверки ограничений ---
        # Формат: (user_id, report_id, query_text)
        test_queries = [
            # --- Тесты для user1 ---
            ("user1", "7.117", "Покажи чистую стоимость и количество фактур за сегодня"),
            # Ожидаемый фильтр: AND ((ZCFO1 in (...) or (ZDIV = '04')))

            ("user1", "7.117", "Покажи сумму отгрузок для дивизиона 02 за прошлый месяц"),
            # Ожидаемый фильтр: WHERE ZDIV = '02' AND ... AND ((ZCFO1 in (...) or (ZDIV = '04')))

            ("user1", "7.999", "Покажи количество уникальных клиентов за вчера"),
             # Ожидаемый фильтр: AND (ZDIV = '03') - правило '7*'

            ("user1", "7.999", "Покажи отгрузки для дивизиона 01 за сегодня"),
             # Ожидаемый фильтр: WHERE ZDIV = '01' AND ... AND (ZDIV = '03') -> Скорее всего, вернет 0 строк

            ("user1", "other_report", "Назови топ 5 клиентов по сумме NETWR за год"),
             # Ожидаемый фильтр: AND (ZDIV = '01') - правило '*'

            ("user1", "other_report", "Покажи отгрузки для дивизиона 03 за сегодня"),
             # Ожидаемый фильтр: WHERE ZDIV = '03' AND ... AND (ZDIV = '01') -> Скорее всего, вернет 0 строк

            # --- Тесты для user2 ---
            ("user2", "7.117", "Покажи сумму чистой стоимости по дивизионам за прошлую неделю"),
             # Ожидаемый фильтр: AND (ZDIV = '01') - правило '7*'

            ("user2", "7.117", "Сколько отгрузили в дивизион 02 вчера?"),
             # Ожидаемый фильтр: WHERE ZDIV = '02' AND ... AND (ZDIV = '01') -> Скорее всего, вернет 0 строк

            ("user2", "other_report", "Покажи количество фактур за сегодня"),
             # Ожидаемый фильтр: AND (1=2) - нет подходящих правил для user2 и '*'/'other_report'

            # --- Тест для пользователя без прав ---
            ("user_no_rights", "7.117", "Покажи что-нибудь"),
             # Ожидаемый фильтр: AND (1=2) - нет правил для этого пользователя
        ]

        print("\n" + "="*30 + f"\nНачало серии тестовых запросов для проверки ограничений\n" + "="*30)

        for i, (user_id, report_id, query) in enumerate(test_queries):
            print(f"\n--- Тест {i+1}: User='{user_id}', Report='{report_id}' ---")
            # Запускаем агент с указанными user_id, report_id и запросом
            agent.run(user_id=user_id, message=query, report_id=report_id)
            print("-" * 20)

        print("\n" + "="*30 + f"\nСерия тестовых запросов завершена\n" + "="*30)


    except ImportError as e:
         print(f"\n!!! Ошибка импорта: {e}. Установите недостающие зависимости.")
    except FileNotFoundError as e:
         print(f"\n!!! Критическая ошибка: Не найден необходимый файл: {e}")
    except Exception as e:
        print(f"\n!!! Критическая ошибка при инициализации или запуске агента: {e}")
        traceback.print_exc()
    finally:
        # Убедимся, что соединение с БД чекпоинтера закрыто
        if agent:
            agent.close_connection()
        print("\nПрограмма завершена.")