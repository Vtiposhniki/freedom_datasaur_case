import json
import re
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd


class FIRE_Engine_V10:
    COLUMN_ALIASES: Dict[str, List[str]] = {
        "guid": ["guid клиента", "guid", "client_guid", "id"],
        "description": ["описание", "текст обращения", "description"],
        "segment": ["сегмент клиента", "сегмент", "segment"],
        "country": ["страна", "country"],
        "city": ["населенный пункт", "город", "city", "населённый пункт"],
        "name": ["фио", "менеджер", "name"],
        "position": ["должность", "позиция", "position"],
        "skills": ["навыки", "skills"],
        "load": ["количество обращений в работе", "кол-во обращений в работе", "нагрузка", "load"],
        "office": ["офис", "бизнес-единица", "unit", "business_unit"],
    }

    def __init__(
        self,
        tickets_df: pd.DataFrame,
        managers_df: pd.DataFrame,
        units_df: pd.DataFrame,
        *,
        enable_fallback: bool = False,
    ):
        self.enable_fallback = enable_fallback

        self.tickets = self._smart_normalize(
            tickets_df, ["guid", "description", "segment", "country", "city"]
        )
        self.managers = self._smart_normalize(
            managers_df, ["name", "position", "skills", "load", "office"]
        )
        self.units = self._smart_normalize(units_df, ["office"])

        self.managers["load"] = pd.to_numeric(
            self.managers["load"], errors="coerce"
        ).fillna(0).astype(int)

        self.managers["name"] = self.managers["name"].astype(str).str.strip()
        self.managers["office"] = self.managers["office"].astype(str).str.strip()
        self.units["office"] = self.units["office"].astype(str).str.strip()

        self.managers["pos_norm"] = (
            self.managers["position"]
            .astype(str)
            .str.lower()
            .str.replace("ё", "е")
            .str.replace(".", "", regex=False)
            .str.replace("специалист", "спец")
            .str.strip()
        )

        self.managers["skills_set"] = self.managers["skills"].apply(self._parse_skills)

        self.astana_office = self._find_canonical_office("астан")
        self.almaty_office = self._find_canonical_office("алмат")

        self.rr_counters: Dict[Tuple, int] = {}
        self.unknown_loc_counter = 0

    # ================= HELPERS =================

    def _smart_normalize(self, df: pd.DataFrame, required: List[str]) -> pd.DataFrame:
        df = df.copy()
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace("ё", "е")
            .str.replace("\u00a0", " ")
        )

        rename_map = {}
        for canonical, aliases in self.COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in df.columns:
                    rename_map[alias] = canonical
                    break

        df = df.rename(columns=rename_map)

        missing = set(required) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        return df

    def _parse_skills(self, x) -> Set[str]:
        if pd.isna(x):
            return set()
        s = str(x).strip()
        if not s or s.lower() == "nan":
            return set()
        return {p.strip().upper() for p in s.replace(";", ",").split(",") if p.strip()}

    def _find_canonical_office(self, pattern: str) -> str:
        mask = self.units["office"].str.lower().str.contains(pattern, na=False)
        found = self.units.loc[mask, "office"].values
        return found[0] if len(found) else pattern.capitalize()

    # ================= ANALYSIS =================

    def analyze_ticket(self, ticket: pd.Series) -> Dict[str, object]:
        text = str(ticket.get("description", "")).lower()
        raw_seg = str(ticket.get("segment", "MASS")).upper()

        if "VIP" in raw_seg:
            segment = "VIP"
        elif "PRIORITY" in raw_seg:
            segment = "PRIORITY"
        else:
            segment = "MASS"

        if re.search(r"[әғқңөұүһіІ]", text):
            lang = "KZ"
        elif re.search(r"[a-z]{3,}", text):
            lang = "ENG"
        else:
            lang = "RU"

        t_type = "Консультация"
        keywords = {
            "Мошеннические действия": ["мошенник", "украли", "фрод", "взлом"],
            "Неработоспособность приложения": ["ошибка", "баг", "не работает", "вылетает"],
            "Претензия": ["претензия", "возврат", "суд", "компенсация"],
            "Смена данных": ["паспорт", "данные", "фио", "смена", "изменить"],
            "Жалоба": ["жалоба", "ужасно", "плохо", "недоволен"],
            "Спам": ["реклама", "выиграли", "приз", "акция"],
        }
        for cat, words in keywords.items():
            if any(w in text for w in words):
                t_type = cat
                break

        priority = 3
        if t_type in ["Мошеннические действия", "Претензия"]:
            priority = 9
        elif t_type in ["Жалоба", "Неработоспособность приложения"]:
            priority = 7

        if segment == "VIP":
            priority = max(priority, 10)
        elif segment == "PRIORITY":
            priority = max(priority, 8)

        return {"type": t_type, "lang": lang, "priority": priority, "segment": segment}

    # ================= DISTRIBUTION =================

    def distribute(self) -> pd.DataFrame:
        results = []

        for _, ticket in self.tickets.iterrows():
            ai = self.analyze_ticket(ticket)
            office = self.get_office(ticket)

            subset = self.managers[self.managers["office"] == office].copy()

            # 1. VIP filter
            if ai["segment"] in ["VIP", "PRIORITY"]:
                subset = subset[subset["skills_set"].apply(lambda s: "VIP" in s)]

            # 2. Chief specialist filter
            if ai["type"] == "Смена данных":
                subset = subset[
                    subset["pos_norm"].str.contains("глав", na=False) &
                    subset["pos_norm"].str.contains("спец", na=False)
                ]

            # 3. Language filter
            if ai["lang"] in ["KZ", "ENG"]:
                subset = subset[subset["skills_set"].apply(lambda s: ai["lang"] in s)]

            # ===== VIP ESCALATION TO CAPITAL =====
            if ai["segment"] == "VIP" and subset.empty:
                capital_subset = self.managers[
                    self.managers["office"].isin(
                        [self.astana_office, self.almaty_office]
                    )
                ]
                capital_subset = capital_subset[
                    capital_subset["skills_set"].apply(lambda s: "VIP" in s)
                ]
                subset = capital_subset.copy()
                office = "CAPITAL_ESCALATION"

            # ===== FALLBACK =====
            if subset.empty and self.enable_fallback:
                subset = self.managers[self.managers["office"] == office].copy()

            manager_final = "UNASSIGNED"

            if not subset.empty:
                subset = subset.sort_values(["load", "name"])
                top_2 = subset.head(2)

                rr_key = (office, ai["segment"], ai["type"], ai["lang"])
                rr_idx = self.rr_counters.get(rr_key, 0)
                selected = top_2.iloc[rr_idx % len(top_2)]
                self.rr_counters[rr_key] = rr_idx + 1

                manager_final = selected["name"]
                self.managers.at[selected.name, "load"] += 1

            results.append(
                {
                    "guid": ticket.get("guid", "N/A"),
                    "ai_type": ai["type"],
                    "ai_lang": ai["lang"],
                    "priority": ai["priority"],
                    "office": office,
                    "manager": manager_final,
                }
            )

        return pd.DataFrame(results)

    # ================= OFFICE =================

    def get_office(self, ticket: pd.Series) -> str:
        country = str(ticket.get("country", "")).lower()
        country_norm = re.sub(r"[^a-zа-я]+", "", country)
        city = str(ticket.get("city", "")).lower()

        for off in self.units["office"].values:
            root = off.lower().replace("офис", "").strip()
            if root and (root in city or city in root):
                return off

        if not any(x in country_norm for x in ["казахстан", "kazakhstan", "kz"]):
            office = [self.astana_office, self.almaty_office][
                self.unknown_loc_counter % 2
            ]
            self.unknown_loc_counter += 1
            return office

        return self.astana_office