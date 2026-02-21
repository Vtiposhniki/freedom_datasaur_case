"""
LLM Analyzer — Gemini 2.5 Flash
================================
Отвечает ТОЛЬКО за NLP-интерпретацию текста обращения.
Никакой информации о менеджерах, нагрузке, офисах и правилах маршрутизации.

Вход:  client_message (str)
Выход: dict с полями intent, sentiment, suggested_priority, language, summary, recommendation
"""

import json
import os
import re
import time
import logging
from typing import Optional

import google.generativeai as genai

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
VALID_LANGUAGES  = {"RU", "KZ", "ENG"}

SYSTEM_PROMPT = """Ты — аналитик клиентских обращений. Твоя задача — анализировать текст и возвращать строго JSON.

Доступные значения:
- intent: "Жалоба" | "Смена данных" | "Консультация" | "Претензия" | "Неработоспособность приложения" | "Мошеннические действия" | "Спам"
- sentiment: "Позитивный" | "Нейтральный" | "Негативный"
- suggested_priority: целое число от 1 до 10
- language: "RU" | "KZ" | "ENG"
- summary: 1–2 предложения, краткая суть обращения
- recommendation: короткая подсказка специалисту (1–2 предложения)

Правила:
1. Отвечай ТОЛЬКО валидным JSON без markdown-блоков, без пояснений.
2. Для suggested_priority: Мошеннические действия/Претензия → 8–10, Жалоба/Неработоспособность → 6–8, остальные → 1–5.
3. Определяй язык по тексту: KZ если есть казахские буквы (ә,ғ,қ,ң,ө,ұ,ү,һ,і), ENG если латиница, иначе RU.
4. summary и recommendation пиши на языке обращения (RU/KZ/ENG).
"""

USER_TEMPLATE = 'Проанализируй обращение клиента:\n\n"""\n{message}\n"""'

# ──────────────────────────────────────────────────────────────
#  FALLBACK — если API недоступен
# ──────────────────────────────────────────────────────────────

def _fallback_analysis(message: str) -> dict:
    """Минимальный детерминированный анализ без LLM."""
    text = message.lower()

    # Intent
    intent = "Консультация"
    intent_map = [
        ("Мошеннические действия", ["мошенник", "украли", "фрод", "взлом", "несанкционир"]),
        ("Неработоспособность приложения", ["ошибка", "баг", "не работает", "вылетает", "зависает"]),
        ("Претензия",   ["претензия", "возврат", "суд", "компенсация"]),
        ("Смена данных", ["паспорт", "данные", "фио", "смена", "изменить"]),
        ("Жалоба",      ["жалоба", "ужасно", "плохо", "недоволен", "отвратительно"]),
        ("Спам",        ["реклама", "выиграли", "приз", "акция", "розыгрыш"]),
    ]
    for cat, words in intent_map:
        if any(w in text for w in words):
            intent = cat
            break

    # Priority
    priority_map = {
        "Мошеннические действия": 9,
        "Претензия": 8,
        "Жалоба": 7,
        "Неработоспособность приложения": 7,
        "Смена данных": 5,
        "Консультация": 3,
        "Спам": 1,
    }
    priority = priority_map.get(intent, 3)

    # Sentiment
    neg_words = ["плохо", "ужасно", "недоволен", "злой", "возмущен", "мошенник", "украли"]
    pos_words = ["спасибо", "отлично", "хорошо", "помогли", "доволен"]
    if any(w in text for w in neg_words):
        sentiment = "Негативный"
    elif any(w in text for w in pos_words):
        sentiment = "Позитивный"
    else:
        sentiment = "Нейтральный"

    # Language
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
        "summary": f"Обращение типа «{intent}». Требует обработки.",
        "recommendation": f"Обработать как «{intent}». Проверить детали запроса.",
        "_source": "fallback",
    }


# ──────────────────────────────────────────────────────────────
#  ВАЛИДАЦИЯ ОТВЕТА
# ──────────────────────────────────────────────────────────────

def _validate_and_fix(data: dict, original_message: str) -> dict:
    """Валидирует и исправляет ответ модели."""

    if data.get("intent") not in VALID_INTENTS:
        logger.warning(f"Invalid intent '{data.get('intent')}', using fallback")
        fallback = _fallback_analysis(original_message)
        data["intent"] = fallback["intent"]

    if data.get("sentiment") not in VALID_SENTIMENTS:
        data["sentiment"] = "Нейтральный"

    try:
        p = int(data.get("suggested_priority", 3))
        data["suggested_priority"] = max(1, min(10, p))
    except (TypeError, ValueError):
        data["suggested_priority"] = 3

    if data.get("language") not in VALID_LANGUAGES:
        data["language"] = "RU"

    if not data.get("summary"):
        data["summary"] = "Описание отсутствует."

    if not data.get("recommendation"):
        data["recommendation"] = "Обработать стандартно."

    # Убираем служебные поля от fallback если вдруг попали
    data.pop("_source", None)

    return data


# ──────────────────────────────────────────────────────────────
#  ОСНОВНОЙ КЛАСС
# ──────────────────────────────────────────────────────────────

class GeminiAnalyzer:
    """
    Анализирует текст обращения через Gemini 2.5 Flash.
    Не знает ничего о менеджерах, офисах и бизнес-правилах.
    """

    MODEL_NAME    = "gemini-2.5-flash-preview-04-17"
    MAX_RETRIES   = 3
    RETRY_DELAY   = 2.0  # секунды

    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.getenv("GEMINI_API_KEY")
        if not key:
            logger.warning("GEMINI_API_KEY not set — will use fallback analysis")
            self._client = None
            return

        genai.configure(api_key=key)
        self._model = genai.GenerativeModel(
            model_name=self.MODEL_NAME,
            system_instruction=SYSTEM_PROMPT,
            generation_config=genai.GenerationConfig(
                temperature=0.1,          # детерминированность важнее творчества
                response_mime_type="application/json",
            ),
        )
        self._client = True
        logger.info(f"GeminiAnalyzer initialized with model {self.MODEL_NAME}")

    # ── PUBLIC API ──────────────────────────────────────────

    def analyze(self, client_message: str) -> dict:
        """
        Анализирует одно обращение.

        Args:
            client_message: текст обращения клиента

        Returns:
            dict: {
                "intent": str,
                "sentiment": str,
                "suggested_priority": int,
                "language": str,
                "summary": str,
                "recommendation": str
            }
        """
        if not client_message or not str(client_message).strip():
            return self._empty_result()

        message = str(client_message).strip()

        if self._client is None:
            logger.debug("Using fallback (no API key)")
            return _fallback_analysis(message)

        return self._call_with_retry(message)

    def analyze_batch(self, messages: list[str]) -> list[dict]:
        """Анализирует список обращений."""
        return [self.analyze(msg) for msg in messages]

    # ── PRIVATE ─────────────────────────────────────────────

    def _call_with_retry(self, message: str) -> dict:
        prompt = USER_TEMPLATE.format(message=message)
        last_error = None

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = self._model.generate_content(prompt)
                raw = response.text.strip()

                # Убираем markdown-обёртку если модель всё же добавила
                raw = re.sub(r"^```(?:json)?\s*", "", raw)
                raw = re.sub(r"\s*```$", "", raw)

                data = json.loads(raw)
                validated = _validate_and_fix(data, message)
                logger.debug(f"Gemini OK (attempt {attempt}): intent={validated['intent']}")
                return validated

            except json.JSONDecodeError as e:
                last_error = e
                logger.warning(f"Attempt {attempt}: JSON parse error — {e}")

            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt}: API error — {e}")

            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY * attempt)

        logger.error(f"All {self.MAX_RETRIES} attempts failed: {last_error}. Using fallback.")
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