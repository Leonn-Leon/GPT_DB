from langchain_openai import ChatOpenAI
from gpt_db.restriction_for_sql import apply_restrictions
from gpt_db.adding_txt_fields import add_txt_fields
from gpt_db.search_of_near_vectors import search_of_near_vectors
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict
from langchain_core.messages import HumanMessage, AIMessage, RemoveMessage
from langgraph.graph.message import add_messages, REMOVE_ALL_MESSAGES
from langgraph.checkpoint.memory import MemorySaver
from typing import Annotated, Literal
from prompts import system_message_1, system_message_2, system_message_3, system_message_4
from langchain_core.runnables import RunnableConfig
from datetime import date, timedelta
import logging
#logging.basicConfig(level=logging.DEBUG)
current_date = date.today().strftime("%Y%m%d")
yesterday_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

class State(TypedDict):
    messages: Annotated[list, add_messages]
    long_term_memory_of_inc_req: Annotated[list, add_messages]
    question: str
    answer: str
    filters: dict
    sql: str
    comment: str
    auth: str

class GPTAgent:
    def __init__(
        self,
        model: str = "deepseek-chat-v3-0324",
        api_key: str = "sk-aitunnel-GGugVSWn9xV0xyATao3GruRiF3i0QF8z",
        base_url: str = "https://api.aitunnel.ru/v1/"
    ):
        self.llm = ChatOpenAI(
            model=model,
            api_key=api_key,
            base_url=base_url,
            temperature=0, 
        )
        self.memory = MemorySaver()
        self.agent = self._build_agent()
        self.previous_state = None

    def _build_agent(self):
        builder = StateGraph(State)

        # Добавляем ноды
        builder.add_node("generate_query", self._generate_query)
        builder.add_node("get_keys", self._get_keys)
        builder.add_node("generate_sql", self._generate_sql)
        builder.add_node("generate_comment", self._generate_comment)
        builder.add_node("cleaning_of_state", self._cleaning_of_state)

        # Соединяем ноды
        builder.add_edge(START, "generate_query")
        builder.add_conditional_edges(
            "generate_query",
            self._should_continue,
        )
        builder.add_edge("get_keys", "generate_sql")
        builder.add_edge("generate_sql", "generate_comment")
        builder.add_edge("generate_comment", "cleaning_of_state")
        builder.add_edge("cleaning_of_state", END)

        return builder.compile(checkpointer=self.memory)

    # Ноды
    def _generate_query(self, state: State):
        response = self.llm.invoke([system_message_1] + state['long_term_memory_of_inc_req'] + state["messages"], max_tokens=100)
        
        # Работа с долгосрочной памятью
        question = state["messages"][0]
        long_term_memory_of_inc_req = state['long_term_memory_of_inc_req']
        if len(long_term_memory_of_inc_req) >= 20:
            to_long_term_memory_of_inc_req = [RemoveMessage(id=m.id) for m in long_term_memory_of_inc_req[:2]] + [question, response]            
        else:
            to_long_term_memory_of_inc_req = [question, response]
        #print(to_long_term_memory_of_inc_req)
        return {"messages": [response], "long_term_memory_of_inc_req": to_long_term_memory_of_inc_req, 'question': question.content, 'answer': response.content}

    def _should_continue(self, state: State) -> Literal["get_keys", "cleaning_of_state"]:
        last_message = state["messages"][-1].content
        continue_flag = True if '@' in last_message else False
        if continue_flag:
            return "get_keys"
        else:            
            return "cleaning_of_state"

    def _get_keys(self, state: State):
        last_message = state["messages"][-1].content
        request = last_message.split('@')[1]
        request_human = HumanMessage(request)

        filters = self.llm.invoke([system_message_2, request_human]).content
        if filters:
            filters_and_keys = search_of_near_vectors(filters.split(','))
            message = AIMessage(f'Найдены ключи для фильтров: {filters_and_keys}')
        else:
            filters_and_keys = {}
            message = AIMessage('Фильтры не найдены')

        return {"messages": [message], "filters": filters_and_keys, "question": request}

    def _generate_sql(self, state: State, config: RunnableConfig):
        request = state['question']
        filters = state['filters']
        message = HumanMessage(f'Описание запроса: {request}\nФильтры: {filters}')
        system_message_3_copy = system_message_3
        system_message_3_copy.content = system_message_3.content.replace('current_date', current_date)
        system_message_3_copy.content = system_message_3.content.replace('yesterday_date', yesterday_date)

        sql = self.llm.invoke([system_message_3_copy, message]).content
        user = config.get("configurable").get("thread_id")
        sql_with_restriction, auth = apply_restrictions(sql, user)
        #sql_with_restriction = add_txt_fields(sql_with_restriction) # раскоментить, когда будут _TXT поля
        
        message = AIMessage(f'SQL сгенерирован:\n{sql_with_restriction}')
        return {"messages": [message], "sql": sql_with_restriction, "auth": auth}

    def _generate_comment(self, state: State):
        request = state['question']
        sql = state['sql']
        filters = state['filters']
        message = HumanMessage(f'Запрос: {request}\nSQL: {sql}\nФильтры: {filters}')

        comment = self.llm.invoke([system_message_4, message]).content
        message = AIMessage(f'Комментарий сгенерирован: {comment}')
        return {"messages": [message], "comment": comment}
    
    def _cleaning_of_state(self, state: State):
        self.previous_state = state #сохраняем state перед его очисткой и сбросом агента
        
        return {
            "messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES)],
            'question': '',
            'answer': '',
            'filters': '',
            'sql': '',
            'comment': '',
            "auth": ''
        }

    def run(self, user_id: str, message: str):
        """Основной метод для обработки запроса"""
        user_question = HumanMessage(message)

        for step in self.agent.stream(
            {"messages": [user_question]},
            {"configurable": {"thread_id": user_id}},
            stream_mode="values",
        ):
            if step["messages"]:
                step["messages"][-1].pretty_print()
        
        self.previous_state.update({'user_id':user_id})
        return self.previous_state

if __name__ == "__main__":
    agent = GPTAgent()
    while True:
        message = input("Добро пожаловать в АРМ. Задайте ваш вопрос: ")
        if message.lower() in ["quit", "exit", "q", "выход"]:
            print("До свидания!")
            break
        agent.run(user_id='user2', message=message)