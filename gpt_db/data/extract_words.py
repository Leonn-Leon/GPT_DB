import os
import ast
from typing import List, Optional
from langchain_gigachat.chat_models import GigaChat
from langchain_core.messages import SystemMessage, HumanMessage
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from gpt_db.config import gpt_url

class KeywordExtractor:
    DEFAULT_LLM_MODEL = "deepseek-chat-v3-0324" 
    DEFAULT_LLM_TEMPERATURE = 0.1
    DEFAULT_LLM_TIMEOUT = 60

    def __init__(self,
                 api_key: Optional[str] = None,
                 llm_model: Optional[str] = None,
                 llm_temperature: Optional[float] = None,
                 llm_timeout: Optional[int] = None):
        """
        Инициализирует извлекатель ключевых слов.

        Args:
            api_key (Optional[str]): Учетные данные для GigaChat.
                                                 Если None, пытается загрузить из переменной окружения API_KEY.
            llm_model (Optional[str]): Модель GigaChat для использования.
            llm_temperature (Optional[float]): Температура для генерации LLM.
            llm_timeout (Optional[int]): Таймаут для запросов к LLM.
        """
        load_dotenv()
        if api_key is None:
            self.API_KEY = os.getenv("API_KEY")
            if not self.API_KEY:
                print("Предупреждение: API_KEY не установлены в переменных окружения и не переданы в конструктор.")
        else:
            self.API_KEY = api_key

        self.llm_model = llm_model or self.DEFAULT_LLM_MODEL
        self.llm_temperature = llm_temperature if llm_temperature is not None else self.DEFAULT_LLM_TEMPERATURE
        self.llm_timeout = llm_timeout or self.DEFAULT_LLM_TIMEOUT

        self.llm: GigaChat = self._initialize_llm()

    def _initialize_llm(self) -> GigaChat:
        """Инициализирует LLM модель GigaChat."""
        try:
            llm = ChatOpenAI(
                api_key=self.API_KEY,
                model=self.llm_model,
                temperature=self.llm_temperature,
                timeout=self.llm_timeout,
                base_url=gpt_url
            )
            print(f"GigaChat LLM ({self.llm_model}) инициализирован успешно.")
            return llm
        except Exception as e:
            print(f"Ошибка при инициализации GigaChat: {e}")
            print("Убедитесь, что API_KEY верны, модель доступна и указан правильный scope.")
            raise

    def extract_keywords(self, user_query: str) -> List[str]:
        """
        Извлекает ключевые слова из запроса пользователя с помощью LLM.

        Args:
            user_query (str): Запрос пользователя.

        Returns:
            List[str]: Список извлеченных ключевых слов.
        """
        sys_msg_content = """
        Твоя задача - извлечь из запроса пользователя ключевые слова, которые могут быть использованы для поиска в справочниках или базах данных.
        Игнорируй приветствия, общие фразы и слова, не несущие смысловой нагрузки для поиска (например, "покажи", "мне", "за", "сегодня", "пожалуйста").
        Ключевые слова должны быть в начальной форме (лемматизированы, например, "отгрузки" вместо "отгрузок") и в нижнем регистре.
        Если подходящих ключевых слов нет, верни пустой список.
        Верни результат строго в виде списка Python строк. Например: ['ключ1', 'ключ2'].
        Для запроса "привет, покажи мне отгрузки за сегодня на урале" правильный ответ: ['отгрузки', 'урал'].
        Для запроса "какие были продажи в москве и санкт-петербурге за прошлый месяц?" правильный ответ: ['продажи', 'москва', 'санкт-петербург'].
        Для запроса "спасибо, больше ничего не нужно" правильный ответ: [].
        """
        human_msg_content = f"Извлеки ключевые слова из этого запроса: \"{user_query}\""

        conversation = [
            SystemMessage(content=sys_msg_content),
            HumanMessage(content=human_msg_content)
        ]

        print(f"\nВызов LLM для извлечения ключевых слов из: '{user_query}'...")
        try:
            response = self.llm.invoke(conversation)
            response_content = response.content.strip()
            print(f"Ответ LLM: {response_content}")

            # Попытка распарсить ответ как список Python
            try:
                # Используем ast.literal_eval для безопасного парсинга строки в список
                keywords = ast.literal_eval(response_content)
                if isinstance(keywords, list) and all(isinstance(kw, str) for kw in keywords):
                    return keywords
                else:
                    print(f"Ошибка: LLM вернул некорректный формат. Ожидался список строк, получено: {response_content}")
                    return [] # Возвращаем пустой список в случае неверного формата
            except (SyntaxError, ValueError) as e:
                print(f"Ошибка парсинга ответа LLM как списка: {e}. Ответ: {response_content}")
                # Альтернативно, можно попытаться извлечь слова, если ответ не в формате списка,
                # но это менее надежно и зависит от того, как LLM может "ошибиться".
                # Например, если LLM вернет "отгрузки, урал", можно попытаться .split(', ')
                # Но для строгости лучше ожидать формат списка.
                return []

        except Exception as e:
            print(f"Ошибка при вызове LLM: {e}")
            return [] # Возвращаем пустой список в случае ошибки вызова LLM

# --- Пример использования ---
if __name__ == "__main__":

    try:
        extractor = KeywordExtractor()

        queries = [
            "привет, покажи мне отгрузки за сегодня на урале",
            "Какие у нас были продажи в Москве и Санкт-Петербурге за прошлый месяц?",
            "Нужны данные по остаткам на складе в Новосибирске",
            "Покажи счета на оплату от ООО Ромашка",
            "Спасибо, это всё",
            "Просто интересно, какая погода в Сочи" # Пример, где ключевых слов для справочников может не быть
        ]

        for query in queries:
            keywords = extractor.extract_keywords(query)
            print(f"Запрос: \"{query}\" -> Ключевые слова: {keywords}")
            print("-" * 30)

    except Exception as e:
        print(f"Произошла ошибка при выполнении примера: {e}")