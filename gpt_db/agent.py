import os
import json
import datetime
import yaml
import sqlite3 
import traceback
from typing import List, Annotated, Union, Dict, Optional
from typing_extensions import TypedDict

# --- Langchain & Langgraph Imports ---
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph.message import add_messages

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, BaseMessage
from langchain_gigachat.chat_models import GigaChat
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

from sqlglot import parse_one, condition
from sqlglot.errors import ParseError

from datetime import date, timedelta
from datetime import date

# Импортируем функции напрямую
from gpt_db.restriction_for_sql import apply_restrictions
from gpt_db.search_of_near_vectors import search_of_near_vectors
from gpt_db.config import gpt_url

# --- Определение состояния графа ---
class MessagesState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    final_instruction: Optional[str]
    sql_query: Optional[str]
    user_id: Optional[str]
    report_id: Optional[str]
    restrictions_applied: bool
    needs_clarification: bool
    filters: dict
    relevance_decision: Optional[str]


class GPTAgent:
    """
    Класс GPT-агента для генерации SQL-запросов к БД отгрузок на основе диалога,
    с применением ограничений доступа.
    Использует явную инициализацию SqliteSaver.
    """
    def __init__(self,
                 config_file: str = "gpt_db/data/confs/config.yaml",
                 structure_file: str = 'gpt_db/data/confs/otgruzki_structure.yaml',
                 divisions_file: str = 'gpt_db/data/confs/divisions.txt',
                 checkpoint_db: str = "checkpoints.sqlite",
                 llm_model: str = "deepseek-chat-v3-0324",
                 llm_temperature: float = 0,
                 llm_timeout: int = 600):
        
        load_dotenv()

        # --- Присваивание атрибутов ---
        self.config_file = config_file
        self.structure_file = structure_file
        self.divisions_file = divisions_file
        self.checkpoint_db = checkpoint_db
        self.llm_model = llm_model
        self.llm_temperature = llm_temperature
        self.llm_timeout = llm_timeout
        self.memory = None
        self._sqlite_conn = None

        self.db_schema, self.divisions = self._load_config_and_data()

        self.llm = self._initialize_llm()
        
        self._initialize_prompts()

        try:
            self._sqlite_conn = sqlite3.connect(self.checkpoint_db, check_same_thread=False)
            print(f"SQLite соединение к '{self.checkpoint_db}' создано (check_same_thread=False).")
            self.memory = SqliteSaver(conn=self._sqlite_conn)
            print(f"Чекпоинтер SqliteSaver инициализирован с явным соединением.")
            if not isinstance(self.memory, SqliteSaver):
                 raise TypeError(f"Ошибка: Инициализация вернула {type(self.memory)}, ожидался SqliteSaver.")
            print(f"Тип self.memory: {type(self.memory)}")
        except Exception as e:
            print(f"Ошибка при инициализации SqliteSaver для '{self.checkpoint_db}': {e}")
            self.close_connection()
            raise

        try:
            self.graph = self._build_graph()
            self.compiled_agent = self.graph.compile(checkpointer=self.memory)
            print("Граф успешно скомпилирован.")
        except Exception as e:
             print(f"Ошибка при компиляции графа: {e}")
             self.close_connection()
             raise

    def _initialize_prompts(self):
        """
        Загружает все промпты из секции 'prompts' в config.yaml
        и создает для каждого атрибут self.prompt_<имя_промпта>.
        Этот метод вызывается один раз при инициализации агента.
        """
        print("Инициализация промптов из конфигурации...")
        try:
            prompts_config = self.config['prompts']
                
            if not isinstance(prompts_config, dict):
                    raise TypeError("Секция 'prompts' в config.yaml должна быть словарем (dict).")

            for key, value in prompts_config.items():
                attribute_name = f"prompt_{key}"
                setattr(self, attribute_name, value)
                print(f"  - Промпт '{key}' успешно загружен в self.{attribute_name}")
                
            print("Все промпты успешно инициализированы.")

        except KeyError:
            print("\nКРИТИЧЕСКАЯ ОШИБКА: Секция 'prompts' не найдена в вашем config.yaml!")
            raise
        except Exception as e:
            print(f"\nКРИТИЧЕСКАЯ ОШИБКА при инициализации промптов: {e}")
            raise
            
    def close_connection(self):
        if self._sqlite_conn:
            try:
                print(f"Закрытие соединения с БД чекпоинтера '{self.checkpoint_db}'...")
                self._sqlite_conn.close()
                self._sqlite_conn = None
                print("Соединение закрыто.")
            except Exception as e:
                print(f"Ошибка при закрытии соединения с БД чекпоинтера: {e}")

    def _load_config_and_data(self) -> tuple[str, str]:
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
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
        return db_schema, divisions_data

    def _initialize_llm(self) -> ChatOpenAI:
        try:
            llm = ChatOpenAI(
                model=self.llm_model,
                temperature=self.llm_temperature, timeout=self.llm_timeout,
                base_url=gpt_url
            )
            print(f"deepseek LLM ({self.llm_model}) инициализирован успешно.")
            return llm
        except Exception as e:
            print(f"Ошибка при инициализации LLM: {e}")
            raise
    

    def handle_greeting(self, state: MessagesState) -> MessagesState:
        """Узел, который обрабатывает приветствия."""
        print("--- Узел: handle_greeting ---")
        greeting_response = (
            "Здравствуйте! Я ваш SQL-ассистент.\n\n"
            "Я могу помочь вам получить данные по отгрузкам. "
            "Просто задайте вопрос, например:\n"
            "- *Какая выручка была вчера?*\n"
            "- *Покажи топ 5 клиентов по объему за этот месяц.*\n\n"
            "Что бы вы хотели узнать?"
        )
        return {"messages": [AIMessage(content=greeting_response)]}

    def handle_irrelevant_question(self, state: MessagesState) -> MessagesState:
        """Узел, который обрабатывает нерелевантные вопросы."""
        print("--- Узел: handle_irrelevant_question ---")
        response_text = (
            "Я — специализированный ассистент и могу отвечать на вопросы, "
            "связанные только с базой данных отгрузок. "
            "Например, вы можете спросить о выручке, объеме или клиентах."
        )
        return {"messages": [AIMessage(content=response_text)]}
    
    def handle_chitchat(self, state: MessagesState) -> MessagesState:
        """Узел, который обрабатывает простые благодарности."""
        print("--- Узел: handle_chitchat ---")
        
        response_text = "Пожалуйста! Рад был помочь."

        return {"messages": [AIMessage(content=response_text)]}


    def check_relevance(self, state: MessagesState) -> str:
        """
        Первый этап валидации: проверяет, относится ли вопрос к БД отгрузок.
        Возвращает решение для условной маршрутизации.
        """
        print("\n--- Узел-валидатор: check_relevance ---")
        last_user_message = state["messages"][-1]
        
        if not isinstance(last_user_message, HumanMessage):
            return {"relevance_decision": "proceed"}
        
        sys_prompt = self.prompt_check_relevance_prompt # <-- Используем новый промпт
        
        try:
            response = self.llm.invoke([
                SystemMessage(content=sys_prompt),
                last_user_message
            ]).content.strip().upper()

            print(f"Результат проверки на релевантность: {response}")

            if response == "ДА":
                decision = "proceed"  # Релевантно, продолжаем обработку
            else:
                decision = "irrelevant" # Нерелевантно, отправляем на стандартный ответ

        except Exception as e:
            print(f"Ошибка при проверке релевантности: {e}. Продолжаем по стандартному пути.")
            decision = "proceed"
        
        return {"relevance_decision": decision}
        
        
    def validate_db_query(self, state: MessagesState) -> MessagesState:
        """Узел, который выполняет основную валидацию запроса к БД."""
        print("--- Узел: validate_db_query ---")
        current_messages = state["messages"]
        sys_prompt = self.prompt_validate_instruction
        convo = [SystemMessage(content=sys_prompt)] + current_messages
        result = self.llm.invoke(convo).content.strip()

        if result.startswith("OK:"):
            final_instruction = result.removeprefix("OK:").strip()
            print(f"Инструкция валидна: '{final_instruction}'")
            return {
                "final_instruction": final_instruction,
                "needs_clarification": False
            }
        else:
            print(f"Инструкция требует уточнений: {result}")
            # А вот уточняющий вопрос мы, наоборот, ДОЛЖНЫ добавить в историю.
            return {
                "messages": [AIMessage(content=result)],
                "final_instruction": None,
                "needs_clarification": True
            }

    def route_request(self, state: MessagesState) -> str:
        """Главный маршрутизатор, определяет намерение и возвращает название следующего узла."""
        print("\n--- Узел-маршрутизатор: route_request ---")
        
        history = state["messages"]

        human_like_history = [
            msg for msg in history 
            if isinstance(msg, HumanMessage) or (isinstance(msg, AIMessage) and "===" not in msg.content)
        ]

        intent_checker_prompt = self.prompt_intent_detector
        try:
            intent = self.llm.invoke([
                SystemMessage(content=intent_checker_prompt),
            ] + human_like_history).content.strip().upper()  # <-- используем human_like_history

            print(f"Определено намерение пользователя: {intent}")

            if intent in ["GREETING", "DATABASE_QUESTION", "CHITCHAT"]:
                return intent.lower()
        except Exception as e:
            print(f"Ошибка при определении намерения: {e}. Продолжаем по стандартному пути.")

        return "database_question"

        
    
    def get_keys(self, state: MessagesState):
        """
        ИЗМЕНЕНИЕ: Этот узел теперь не добавляет сообщение в историю.
        Он молча выполняет свою работу и обновляет 'filters' в состоянии.
        Отладочный print остается.
        """
        print("\n--- Узел: get_keys ---")
        sys_prompt = SystemMessage(content=self.prompt_filters_search)
        validated_instruction = state.get('final_instruction')
        
        if not validated_instruction:
            print("Фильтры не найдены (нет инструкции).")
            return {"filters": {}} # Просто возвращаем пустые фильтры

        validated_instruction_human = HumanMessage(validated_instruction)
        
        filters_str = self.llm.invoke([sys_prompt, validated_instruction_human]).content
        if filters_str and not filters_str.isspace():
            filters_and_keys = search_of_near_vectors(filters_str.split(','))
            print(f'Найдены ключи для фильтров: {filters_and_keys}')
        else:
            filters_and_keys = {}
            print('Фильтры не найдены')

        # Возвращаем ТОЛЬКО обновленное состояние, без "messages"
        return {"filters": filters_and_keys}

    def generate_sql_query(self, state: MessagesState) -> Dict:
        """
        ИЗМЕНЕНИЕ: Генерирует SQL-запрос, динамически внедряя актуальную дату.
        1. Получает актуальную дату.
        2. Формирует промпт с этой датой.
        3. Генерирует "сырой" SQL-запрос.
        4. К нему применяется функция apply_restrictions.
        5. В состояние сохраняется финальный SQL.
        """
        print("\n--- Узел: generate_sql_query (с динамической датой и ограничениями) ---")
        validated_instruction = state.get('final_instruction')
        filters = state.get('filters')
        user_id = state.get('user_id')
        if not validated_instruction:
            print("Ошибка: Валидированная инструкция отсутствует. Генерация SQL пропущена.")
            return {
                "messages": [AIMessage(content="-- SQL generation skipped (no instruction) --")], 
                "sql_query": "-- SQL generation skipped (no instruction) --",
                "restrictions_applied": False
            }

        today = datetime.date.today()
        
        date_context_info = (
            f"КОНТЕКСТНАЯ ИНФОРМАЦИЯ О ТЕКУЩЕЙ ДАТЕ:\n"
            f"- Сегодняшняя дата (для current_date): {today.strftime('%Y-%m-%d')}\n"
            f"- Вчерашняя дата (для current_date - 1): {(today - datetime.timedelta(days=1)).strftime('%Y%m%d')}\n"
            f"Используй эту информацию для всех относительных запросов ('вчера', 'сегодня').\n"
            f"----------------------------------\n\n"
        )

        print(f"Инструкция для генерации SQL: {validated_instruction}")

        base_prompt = self.prompt_generate_sql_query.replace("<otgruzki_structure>", self.db_schema)
        final_prompt_with_date = date_context_info + base_prompt
        
        conversation = [
            SystemMessage(content=final_prompt_with_date), 
            HumanMessage(f"Описание запроса: {validated_instruction}\nФильтры: {filters}")
        ]
        try:
            print("Вызов LLM для генерации SQL...")
            response = self.llm.invoke(conversation)
            sql_query = response.content.strip()
            if sql_query.startswith("```sql"): sql_query = sql_query[6:]
            if sql_query.endswith("```"): sql_query = sql_query[:-3]
            sql_query = sql_query.strip()
            print(f"Сгенерирован сырой SQL: \n{sql_query}")
        except Exception as e:
            print(f"Ошибка при вызове LLM в generate_sql_query: {e}")
            return {
                "messages": [AIMessage(content=f"Произошла ошибка при генерации SQL: {e}")],
                "sql_query": f"-- Ошибка генерации SQL: {e} --",
                "restrictions_applied": False
            }

        if not user_id:
            print("Критическая ошибка: user_id отсутствует. Ограничения не могут быть применены.")
            final_sql = sql_query
            restrictions_applied_flag = False
        else:
            print(f"Применение ограничений для user='{user_id}'...")
            try:
                final_sql, restrictions_applied_flag = apply_restrictions(sql_query, user_id)
                if restrictions_applied_flag:
                    print(f"Ограничения успешно применены. Итоговый SQL:\n{final_sql}")
                else:
                    print("Ограничения не были применены (не требуется или ошибка в apply_restrictions).")
            except Exception as e:
                print(f"Ошибка при применении ограничений: {e}")
                final_sql = sql_query # В случае ошибки используем исходный запрос
                restrictions_applied_flag = False

        return {
            "sql_query": final_sql,
            "restrictions_applied": restrictions_applied_flag
        }


    def _get_date_from_instruction(self, instruction: str) -> str:
        """Гибридный метод: LLM извлекает команду, Python вычисляет дату."""
        print("--- Гибридный узел: _get_date_from_instruction ---")
        
        period_extractor_prompt = self.prompt_period_extractor
        
        try:
            # Шаг 1: LLM извлекает команду в JSON
            response_str = self.llm.invoke([
                SystemMessage(content=period_extractor_prompt),
                HumanMessage(content=f"Запрос: {instruction}")
            ]).content.strip()
            print(f"LLM извлекла команду: '{response_str}'")

            # Очищаем ответ от возможных артефактов, например ```json
            if response_str.startswith("```json"):
                response_str = response_str[7:].strip()
            if response_str.endswith("```"):
                response_str = response_str[:-3].strip()

            period_data = json.loads(response_str)
            period_type = period_data.get("type")

            # Шаг 2: Python вычисляет дату на основе команды
            today = date.today()
            
            if period_type == "relative_day":
                value = int(period_data.get("value", 0))
                target_date = today + timedelta(days=value)
                return f"за {target_date.strftime('%d %B %Y года')}"
            
            elif period_type == "last_month":
                first_day_current_month = today.replace(day=1)
                last_day_last_month = first_day_current_month - timedelta(days=1)
                first_day_last_month = last_day_last_month.replace(day=1)
                start_str = first_day_last_month.strftime('%d.%m.%Y')
                end_str = last_day_last_month.strftime('%d.%m.%Y')
                return f"за период с {start_str} по {end_str}"

            elif period_type == "current_month":
                start_str = today.replace(day=1).strftime('%d.%m.%Y')
                end_str = today.strftime('%d.%m.%Y')
                return f"за текущий месяц (с {start_str} по {end_str})"
                
            else: # period_type == "none" или что-то еще
                return "" # Возвращаем пустую строку, если период не найден

        except (json.JSONDecodeError, AttributeError, Exception) as e:
            print(f"Ошибка при парсинге или вычислении даты: {e}")
            return "" # Возвращаем пустую строку в случае любой ошибки
    
    
    def comment_sql_query(self, state: MessagesState) -> Dict[str, List[BaseMessage]]:
        """Генерирует комментарий к SQL-запросу, используя точную дату."""
        print(f"\n--- Узел: comment_sql_query (с определением даты) ---")
        
        sql_query = state.get('sql_query')
        final_instruction = state.get('final_instruction', "[Инструкция не найдена]")
        restrictions_applied = state.get('restrictions_applied', False)
        
        if not sql_query or "error" in sql_query.lower() or "skipped" in sql_query.lower():
            print("SQL-запрос не найден или содержит ошибку. Формирую сообщение об ошибке.")
            # Если SQL не удалось сгенерировать, возвращаем пользователю осмысленную ошибку
            return {"messages": [AIMessage(content=f"К сожалению, не удалось обработать ваш запрос. {sql_query}")]}

        human_readable_date = self._get_date_from_instruction(final_instruction)
        print(f"Получена человекочитаемая дата: '{human_readable_date}'")

        date_info_for_prompt = f"Конкретный период запроса: {human_readable_date}\n\n" if human_readable_date else ""

        sys_msg_content = self.prompt_comment_sql_query
        human_msg_content = (
            f"Вопрос пользователя:\n{final_instruction}\n\n"
            f"{date_info_for_prompt}"  # <-- Используем новую переменную
            f"Структура данных, которые будут извлечены для ответа:\n{sql_query}\n\n"
            f"Флаг применения ограничений видимости (restrictions_applied): {restrictions_applied}\n\n"
            "Сгенерируй шаблон ответа для пользователя согласно правилам."
        )
        conversation = [SystemMessage(sys_msg_content), HumanMessage(human_msg_content)]
        try:
            comment = self.llm.invoke(conversation).content.strip()
        except Exception as e:
            comment = f"-- Ошибка при генерации комментария: {e} --"

        final_content = f"{sql_query}\n===\n{comment or '-- Комментарий не сгенерирован --'}"
        return {"messages": [AIMessage(content=final_content)]}
    

    def _build_graph(self) -> StateGraph:
        """
        Собирает граф с маршрутизацией на основе намерения пользователя.
        """
        workflow = StateGraph(MessagesState)

        # --- ШАГ 1: Добавляем все узлы, КРОМЕ маршрутизатора ---
        workflow.add_node("handle_greeting", self.handle_greeting)
        workflow.add_node("handle_chitchat", self.handle_chitchat)
        workflow.add_node("check_relevance", self.check_relevance) 
        workflow.add_node("handle_irrelevant_question", self.handle_irrelevant_question)
        workflow.add_node("validate_db_query", self.validate_db_query)
        workflow.add_node("get_keys", self.get_keys)
        workflow.add_node("generate_sql_query", self.generate_sql_query)
        workflow.add_node("comment_sql_query", self.comment_sql_query)
        
        # --- ШАГ 2: Устанавливаем точку входа ---
        # Точка входа по-прежнему определяет базовое намерение
        workflow.set_conditional_entry_point(
            self.route_request,
            {
                "greeting": "handle_greeting",
                "chitchat": "handle_chitchat",
                "database_question": "check_relevance" 
            }
        )
        
        # --- ШАГ 3: Определяем связи от конечных точек веток ---
        
        # Ветки "болтовни" сразу заканчиваются
        workflow.add_edge("handle_greeting", END)
        workflow.add_edge("handle_chitchat", END)
        workflow.add_edge("handle_irrelevant_question", END) # Новая конечная ветка

        def route_after_relevance(state: MessagesState) -> str:
            """Читает решение из состояния и возвращает название следующего узла."""
            return state.get("relevance_decision", "proceed")

        workflow.add_conditional_edges(
            "check_relevance",          # Начинаем с этого узла
            route_after_relevance,      # Используем эту функцию для принятия решения
            {
                "proceed": "validate_db_query",
                "irrelevant": "handle_irrelevant_question"
            }
        )
        
        # Ветка валидации имеет свое собственное ветвление
        def route_after_validation(state: MessagesState):
            return "clarify" if state.get("needs_clarification") else "proceed"

        workflow.add_conditional_edges(
            "validate_db_query",
            route_after_validation,
            {
                "clarify": END, # Если нужны уточнения, завершаем
                "proceed": "get_keys" # Если все ок, идем дальше по основной цепочке
            }
        )

        # --- ШАГ 4: Собираем основную цепочку для SQL-запроса ---
        workflow.add_edge("get_keys", "generate_sql_query")
        workflow.add_edge("generate_sql_query", "comment_sql_query")
        workflow.add_edge("comment_sql_query", END)
        
        return workflow

    # --- Функция для запуска диалога ---
    def run(self, user_id: str, message: str, report_id: Optional[str] = "default_report") -> Optional[Dict]:
        thread_id = f"user_{user_id}_{report_id}"
        config = {"configurable": {"thread_id": thread_id}}

        print(f"\n===== Диалог для {thread_id} (User: {user_id}, Report: {report_id}) =====")
        print(f"Пользователь: {message}")

        input_data = {
            "messages": [HumanMessage(content=message)],
            "user_id": user_id,
            "report_id": report_id
        }

        print("\nЗапуск графа (используя invoke)...")
        final_state_values = None
        try:
            final_state_values = self.compiled_agent.invoke(input_data, config=config)
            
            print("\n--- Итоговый ответ ---")
            if final_state_values and final_state_values.get('messages'):
                final_result_message = final_state_values['messages'][-1]
                print(f"Агент:\n{final_result_message.content}")
            else:
                print("Агент: [Нет ответа или произошла ошибка выполнения графа]")

        except Exception as e:
            print(f"\n!!! Ошибка во время выполнения графа (invoke) для {thread_id}: {e}")
            traceback.print_exc()
            final_state_values = {"messages": [AIMessage(content=f"Ошибка выполнения: {e}")]}

        print(f"===== Конец диалога для {thread_id} =====")
        return final_state_values

    def __del__(self):
        self.close_connection()