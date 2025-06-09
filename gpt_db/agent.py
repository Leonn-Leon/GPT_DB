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

# Импортируем функции напрямую
from gpt_db.restriction_for_sql import apply_restrictions
from gpt_db.search_of_near_vectors import search_of_near_vectors
from gpt_db.config import gpt_url

# --- Определение состояния графа ---
class MessagesState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages]
    final_instruction: Optional[str]
    user_id: Optional[str]
    report_id: Optional[str]
    restrictions_applied: bool # Флаг, который теперь устанавливается в узле генерации
    needs_clarification: bool
    filters: dict


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
                 llm_temperature: float = 0.01,
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

        # --- Загрузка конфигурации и данных ---
        self.db_schema, self.divisions = self._load_config_and_data()

        # --- Инициализация LLM ---
        self.llm = self._initialize_llm()

        # --- Настройка чекпоинтера (ЯВНАЯ ИНИЦИАЛИЗАЦИЯ) ---
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

        # --- Сборка и компиляция графа ---
        try:
            self.graph = self._build_graph()
            self.compiled_agent = self.graph.compile(checkpointer=self.memory)
            print("Граф успешно скомпилирован.")
        except Exception as e:
             print(f"Ошибка при компиляции графа: {e}")
             self.close_connection()
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

    # --- Методы загрузки и инициализации (без изменений) ---
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
    
    def validate_instruction(self, state: MessagesState) -> MessagesState:
        """
        ИЗМЕНЕНИЕ: Узел валидации теперь работает по новым, строгим правилам.
        Проверяет только наличие полей для SELECT, игнорирует WHERE.
        Возвращает ответ в формате "OK: <инструкция>".
        """
        print("\n--- Узел: validate_instruction (строгая версия) ---")
        current_messages = state["messages"]
        user_messages = [msg for msg in current_messages if isinstance(msg, HumanMessage)]
        if not user_messages:
            return {"messages": [AIMessage(content="Ошибка: не найдено сообщение пользователя.")], "needs_clarification": True}

        # Используем новый, строгий промпт
        sys_prompt = self.config["validate_instruction"].replace("<otgruzki_structure>", self.db_schema)

        convo = [SystemMessage(content=sys_prompt)] + user_messages 
        result = self.llm.invoke(convo).content.strip()

        # ИЗМЕНЕНИЕ: Парсим новый формат ответа "OK: ..."
        if result.startswith("OK:"):
            final_instruction = result.removeprefix("OK:").strip()
            print(f"Инструкция валидна: '{final_instruction}'")
            return {
                # Сохраняем в историю только сообщение о том, что валидация пройдена
                "messages": [AIMessage(content=f"Валидация пройдена. Инструкция: {final_instruction}")],
                "final_instruction": final_instruction,
                "needs_clarification": False
            }
        else:
            print(f"Инструкция требует уточнений: {result}")
            # Возвращаем уточняющий вопрос от модели
            return {
                "messages": [AIMessage(content=result)],
                "final_instruction": None,
                "needs_clarification": True
            }
    
    def get_keys(self, state: MessagesState):
        """
        ИЗМЕНЕНИЕ: Этот узел теперь не добавляет сообщение в историю.
        Он молча выполняет свою работу и обновляет 'filters' в состоянии.
        Отладочный print остается.
        """
        print("\n--- Узел: get_keys ---")
        sys_prompt = SystemMessage(content=self.config["filters_search"])
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
        ИЗМЕНЕНИЕ (п. 3): В этот узел перенесена логика применения ограничений.
        1. Генерируется "сырой" SQL-запрос.
        2. К нему применяется функция apply_restrictions.
        3. В состояние сохраняется финальный SQL и флаг о том, были ли применены ограничения.
        """
        print("\n--- Узел: generate_sql_query (с применением ограничений) ---")
        validated_instruction = state.get('final_instruction')
        filters = state.get('filters')
        user_id = state.get('user_id')
        
        # Инициализация выходного состояния
        output_state = {"messages": [], "restrictions_applied": False}

        if not validated_instruction:
             print("Ошибка: Валидированная инструкция отсутствует.")
             output_state["messages"] = [AIMessage(content="-- SQL generation skipped (no instruction) --")]
             return output_state

        print(f"Инструкция для генерации SQL: {validated_instruction}")
        sys_msg_content = self.config["generate_sql_query"].replace("<otgruzki_structure>", self.db_schema)
        conversation = [
            SystemMessage(sys_msg_content), 
            HumanMessage(f"Описание запроса: {validated_instruction}\nФильтры: {filters}")
        ]

        # --- Шаг 1: Генерация "сырого" SQL ---
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
            output_state["messages"] = [AIMessage(content=f"Произошла ошибка при генерации SQL: {e}")]
            return output_state

        # --- Шаг 2: Применение ограничений (логика из удаленного узла) ---
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

        # --- Шаг 3: Обновление состояния ---
        output_state["messages"] = [AIMessage(content=final_sql)]
        output_state["restrictions_applied"] = restrictions_applied_flag
        
        return output_state

    def comment_sql_query(self, state: MessagesState) -> Dict[str, List[BaseMessage]]:
        """
        ИЗМЕНЕНИЕ (п. 4): Улучшена логика формирования итогового ответа.
        Теперь разделитель "===" будет всегда, даже если генерация комментария не удалась.
        """
        print(f"\n--- Узел: comment_sql_query ---")
        current_messages = state['messages']
        final_instruction = state.get('final_instruction', "[Инструкция не найдена]")
        restrictions_applied = state.get('restrictions_applied', False)
        
        sql_query = ""
        # Ищем последний AIMessage, который должен содержать итоговый SQL
        if current_messages and isinstance(current_messages[-1], AIMessage):
            content_lower = current_messages[-1].content.lower()
            if "error" not in content_lower and "skipped" not in content_lower:
                sql_query = current_messages[-1].content.strip()
        
        if not sql_query:
            print("SQL-запрос не найден или была ошибка. Комментарий не будет сгенерирован.")
            # Возвращаем последнее сообщение (вероятно, об ошибке) как есть
            return {"messages": current_messages}

        print(f"SQL для комментирования:\n{sql_query}")
        print(f"На основе инструкции: {final_instruction}")
        print(f"Ограничения применены: {restrictions_applied}")

        sys_msg_content = self.config["comment_sql_query"]
        human_msg_content = (
            f"Вопрос пользователя:\n{final_instruction}\n\n"
            f"Структура данных, которые будут извлечены для ответа:\n{sql_query}\n\n"
            f"Флаг применения ограничений видимости (restrictions_applied): {restrictions_applied}\n\n"
            "Сгенерируй шаблон ответа для пользователя согласно правилам."
        )

        conversation = [SystemMessage(sys_msg_content), HumanMessage(human_msg_content)]

        comment = ""
        try:
            print("Вызов LLM для генерации комментария...")
            response = self.llm.invoke(conversation)
            comment = response.content.strip()
            print(f"Сгенерирован комментарий: {comment}")
        except Exception as e:
            print(f"Ошибка при вызове LLM в comment_sql_query: {e}")
            comment = f"-- Ошибка при генерации комментария: {e} --"

        # ИЗМЕНЕНИЕ (п. 4): Гарантируем наличие разделителя "==="
        if not comment or comment.isspace():
            comment = "-- Комментарий не был сгенерирован. --"
            
        final_content = f"{sql_query}\n===\n{comment}"
        
        return {"messages": [AIMessage(content=final_content)]}

    # --- Сборка графа ---
    def _build_graph(self) -> StateGraph:
        """
        ИЗМЕНЕНИЕ (п. 3): Граф обновлен. Узел apply_sql_restrictions удален.
        """
        workflow = StateGraph(MessagesState)

        workflow.add_node("validate_instruction", self.validate_instruction)
        workflow.add_node("get_keys", self.get_keys)
        workflow.add_node("generate_sql_query", self.generate_sql_query)
        # УДАЛЕНО: workflow.add_node("apply_sql_restrictions", self._apply_sql_restrictions)
        workflow.add_node("comment_sql_query", self.comment_sql_query)
        
        workflow.set_entry_point("validate_instruction")

        def route_after_validation(state: MessagesState):
            return "clarify" if state.get("needs_clarification") else "proceed"

        workflow.add_conditional_edges(
            "validate_instruction",
            route_after_validation,
            {
                "clarify": END,      # Если нужны уточнения, завершаем работу
                "proceed": "get_keys"  # Если все хорошо, ищем ключи-фильтры
            }
        )

        workflow.add_edge("get_keys", "generate_sql_query")
        # ИЗМЕНЕНИЕ: Прямая связь от генерации к комментированию
        workflow.add_edge("generate_sql_query", "comment_sql_query")
        # УДАЛЕНО: workflow.add_edge("apply_sql_restrictions", "comment_sql_query")
        workflow.add_edge("comment_sql_query", END)
        
        return workflow

    # --- Функция для запуска диалога (без изменений) ---
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