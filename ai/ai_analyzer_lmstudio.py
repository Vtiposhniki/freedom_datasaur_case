"""
LLM Analyzer — Llama 3.1 8B (via LM Studio)
===========================================
Отвечает ТОЛЬКО за NLP-интерпретацию текста обращения.
Никакой информации о менеджерах, нагрузке, офисах и правилах маршрутизации.
"""

import json
import re
import time
import logging
from typing import Optional

# Используем официальную библиотеку OpenAI для работы с LM Studio
from openai import OpenAI, APIConnectionError, APITimeoutError

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
#  КОНСТАНТЫ
# ──────────────────────────────────────────────────────────────

VALID_INTENTS = {
    "Жалоба",
    "Смена данных",
    "Консультация",
    "Претензия",
    "Неработоспособность приложения",
    "Мошеннические действия",
    "Спам",
}

VALID_SENTIMENTS = {"Позитивный", "Нейтральный", "Негативный"}
VALID_LANGUAGES = {"RU", "KZ", "ENG"}

# Для 8B моделей лучше прямо показать ожидаемую структуру JSON
SYSTEM_PROMPT = """Ты — аналитик клиентских обращений. Твоя задача — анализировать текст и возвращать строго JSON.

Доступные значения для полей:
- intent: "Жалоба" | "Смена данных" | "Консультация" | "Претензия" | "Неработоспособность приложения" | "Мошеннические действия" | "Спам"
- sentiment: "Позитивный" | "Нейтральный" | "Негативный"
- suggested_priority: целое число от 1 до 10 (Мошеннические действия/Претензия -> 8-10, Жалоба/Неработоспособность -> 6-8, остальные -> 1-5)
- language: "RU" | "KZ" | "ENG" (KZ если есть ә,ғ,қ,ң,ө,ұ,ү,һ,і; ENG если латиница; иначе RU)
- summary: 1-2 предложения, краткая суть
- recommendation: 1-2 предложения, подсказка специалисту

ОБЯЗАТЕЛЬНО отвечай в формате JSON следующей структуры:
{
  "intent": "...",
  "sentiment": "...",
  "suggested_priority": 5,
  "language": "...",
  "summary": "...",
  "recommendation": "..."
}
Никакого текста до или после JSON.
"""


# ──────────────────────────────────────────────────────────────
#  FALLBACK — если LM Studio недоступен
# ──────────────────────────────────────────────────────────────

def _fallback_analysis(message: str) -> dict:
    """Минимальный детерминированный анализ без LLM."""
    text = message.lower()
    intent = "Консультация"

    intent_map = [
        ("Мошеннические действия", ["мошенник", "украли", "фрод", "взлом", "несанкционир"]),
        ("Неработоспособность приложения", ["ошибка", "баг", "не работает", "вылетает", "зависает"]),
        ("Претензия", ["претензия", "возврат", "суд", "компенсация"]),
        ("Смена данных", ["паспорт", "данные", "фио", "смена", "изменить"]),
        ("Жалоба", ["жалоба", "ужасно", "плохо", "недоволен", "отвратительно"]),
        ("Спам", ["реклама", "выиграли", "приз", "акция", "розыгрыш"]),
    ]
    for cat, words in intent_map:
        if any(w in text for w in words):
            intent = cat
            break

    priority_map = {
        "Мошеннические действия": 9, "Претензия": 8, "Жалоба": 7,
        "Неработоспособность приложения": 7, "Смена данных": 5,
        "Консультация": 3, "Спам": 1,
    }
    priority = priority_map.get(intent, 3)

    if any(w in text for w in ["плохо", "ужасно", "недоволен", "злой", "мошенник", "украли"]):
        sentiment = "Негативный"
    elif any(w in text for w in ["спасибо", "отлично", "хорошо", "помогли", "доволен"]):
        sentiment = "Позитивный"
    else:
        sentiment = "Нейтральный"

    if re.search(r"[әғқңөұүһі]", text):
        language = "KZ"
    elif re.search(r"[a-z]{3,}", text):
        language = "ENG"
    else:
        language = "RU"

    return {
        "intent": intent,
        "sentiment": sentiment,
        "suggested_priority": priority,
        "language": language,
        "summary": f"Обращение типа «{intent}».",
        "recommendation": "Проверить детали запроса."
    }


# ──────────────────────────────────────────────────────────────
#  ВАЛИДАЦИЯ ОТВЕТА
# ──────────────────────────────────────────────────────────────

def _validate_and_fix(data: dict, original_message: str) -> dict:
    if data.get("intent") not in VALID_INTENTS:
        logger.warning(f"Invalid intent '{data.get('intent')}', using fallback")
        data["intent"] = _fallback_analysis(original_message)["intent"]

    if data.get("sentiment") not in VALID_SENTIMENTS:
        data["sentiment"] = "Нейтральный"

    try:
        data["suggested_priority"] = max(1, min(10, int(data.get("suggested_priority", 3))))
    except (TypeError, ValueError):
        data["suggested_priority"] = 3

    if data.get("language") not in VALID_LANGUAGES:
        data["language"] = "RU"

    data["summary"] = data.get("summary", "Описание отсутствует.")
    data["recommendation"] = data.get("recommendation", "Обработать стандартно.")

    return data


# ──────────────────────────────────────────────────────────────
#  ОСНОВНОЙ КЛАСС
# ──────────────────────────────────────────────────────────────

class LocalLLMAnalyzer:
    """
    Анализирует текст через локальную Llama 3.1 (LM Studio).
    """
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0  # Локальной модели большие паузы не нужны

    def __init__(self, base_url: str = "http://10.225.177.226:1234/v1"):
        """
        Инициализация клиента.
        base_url — стандартный адрес сервера LM Studio.
        """
        self._client = OpenAI(
            base_url=base_url,
            api_key="lm-studio"  # Ключ не важен для локального сервера
        )
        logger.info(f"LocalLLMAnalyzer initialized targeting {base_url}")

    # ── PUBLIC API ──────────────────────────────────────────

    def analyze(self, client_message: str) -> dict:
        if not client_message or not str(client_message).strip():
            return self._empty_result()
        return self._call_with_retry(str(client_message).strip())

    def analyze_batch(self, messages: list[str]) -> list[dict]:
        """
        Анализирует список обращений последовательно.
        Для локальной LLM одновременные запросы могут переполнить VRAM или очередь,
        поэтому обрабатываем по одному.
        """
        results = []
        for msg in messages:
            results.append(self.analyze(msg))
        return results

    # ── PRIVATE ─────────────────────────────────────────────

    def _call_with_retry(self, message: str) -> dict:
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                # Отправляем запрос в LM Studio
                response = self._client.chat.completions.create(
                    model="local-model",  # В LM Studio имя модели обычно игнорируется
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": f'Проанализируй обращение:\n\n"""\n{message}\n"""'}
                    ],
                    temperature=0.1,
                    # Форсируем JSON-режим (поддерживается в свежих версиях LM Studio)
                    response_format={"type": "json_schema"}
                )

                raw = response.choices[0].message.content.strip()
                data = json.loads(raw)

                validated = _validate_and_fix(data, message)
                logger.debug(f"Local LLM OK (attempt {attempt}): intent={validated['intent']}")
                return validated

            except json.JSONDecodeError as e:
                last_error = e
                logger.warning(f"Attempt {attempt}: JSON parse error — {e}. Raw response: {raw}")

            except (APIConnectionError, APITimeoutError) as e:
                # Если LM Studio выключен или завис
                last_error = e
                logger.error(f"Attempt {attempt}: LM Studio connection error — {e}")
                break  # Нет смысла ретраить, если сервер лежит

            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt}: Local LLM error — {e}")

            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY)

        logger.error(f"Failed after attempts: {last_error}. Using fallback.")
        return _fallback_analysis(message)

    @staticmethod
    def _empty_result() -> dict:
        return {
            "intent": "Консультация",
            "sentiment": "Нейтральный",
            "suggested_priority": 1,
            "language": "RU",
            "summary": "Обращение пустое или отсутствует.",
            "recommendation": "Уточнить запрос у клиента.",
        }

# ──────────────────────────────────────────────────────────────
#  ДЕМО-ЗАПУСК
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Настраиваем базовое логирование, чтобы видеть процесс
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Список тестовых обращений с разными сценариями
    test_messages = [
        # 1. Фрод / Высокий приоритет / RU
        "С моей карты только что списали 50000 тенге, хотя я ничего не покупал! Срочно заблокируйте счет, это мошенники!",

        # 2. Негатив / Ошибка приложения / ENG
        "The app is completely broken. Every time I try to open the transfer page, it crashes. Fix this terrible bug ASAP.",

        # 3. Консультация / Позитив / KZ
        "Сәлеметсіз бе! Кешегі көмегіңіз үшін үлкен рахмет, қолданба өте жақсы жұмыс істеп тұр. Дегенмен бір сұрағым бар еді.",

        # 4. Смена данных / Нейтральный / RU
        "Добрый день. Подскажите, как я могу изменить номер телефона, привязанный к моему личному кабинету?",

        # 5. Спам / RU
        "Поздравляем! Ваш номер выиграл новый iPhone 15 Pro Max! Перейдите по ссылке, чтобы забрать свой приз прямо сейчас!"
    ]

    print("=== Старт анализа обращений ===\n")

    # Инициализация анализатора
    analyzer = LocalLLMAnalyzer()

    # Запуск пакета сообщений
    start_time = time.time()
    results = analyzer.analyze_batch(test_messages)
    end_time = time.time()

    # Вывод результатов
    print("\n=== Результаты ===")
    for i, (msg, res) in enumerate(zip(test_messages, results), 1):
        print(f"\n--- Обращение {i} ---")
        print(f"Текст: {msg}")
        # Красиво печатаем JSON-ответ
        print(json.dumps(res, indent=4, ensure_ascii=False))

    print(f"\nОбработано {len(test_messages)} сообщений за {end_time - start_time:.2f} сек.")