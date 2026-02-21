"""
CSV Parser → PostgreSQL
========================
Читает три CSV файла последовательно и сохраняет в базу данных.

Порядок загрузки:
  1. business_units.csv  → таблица business_units
  2. managers.csv        → таблица managers
  3. tickets.csv         → таблица tickets

Установка зависимостей:
    pip install psycopg2-binary

Запуск:
    python parser.py

Или с параметрами:
    python parser.py --host localhost --port 5432 --user postgres --password secret --dbname ticket_system
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime


# ══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ ПОДКЛЮЧЕНИЯ (можно менять здесь или через аргументы)
# ══════════════════════════════════════════════════════════════
DEFAULT_CONFIG = {
    "host":     "127.0.0.1",
    "port":     5432,
    "user":     "postgres",
    "password": 12341234,
    "dbname":   "datasaurfreedom",
}

# Пути к CSV файлам
CSV_FILES = {
    "business_units": "business_units.csv",
    "managers":       "managers.csv",
    "tickets":        "tickets.csv",
}


# ══════════════════════════════════════════════════════════════
#  DDL — создание таблиц
# ══════════════════════════════════════════════════════════════
DDL = """
CREATE TABLE IF NOT EXISTS business_units (
    id          SERIAL PRIMARY KEY,
    office_name TEXT NOT NULL UNIQUE,
    address     TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS managers (
    id               SERIAL PRIMARY KEY,
    full_name        TEXT NOT NULL,
    position         TEXT,
    business_unit_id INTEGER REFERENCES business_units(id),
    skills           TEXT[],
    active_tickets   INTEGER DEFAULT 0,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tickets (
    id           SERIAL PRIMARY KEY,
    client_guid  TEXT,
    gender       TEXT,
    birth_date   DATE,
    description  TEXT,
    attachment   TEXT,
    segment      TEXT,
    country      TEXT,
    region       TEXT,
    city         TEXT,
    street       TEXT,
    building     TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ai_analysis (
    id                SERIAL PRIMARY KEY,
    ticket_id         INTEGER REFERENCES tickets(id) ON DELETE CASCADE,
    request_type      TEXT,
    tone              TEXT,
    priority_score    SMALLINT,
    language          TEXT DEFAULT 'RU',
    summary           TEXT,
    recommendation    TEXT,
    client_latitude   DOUBLE PRECISION,
    client_longitude  DOUBLE PRECISION,
    nearest_office_id INTEGER REFERENCES business_units(id),
    is_foreign        BOOLEAN DEFAULT FALSE,
    processed_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS assignments (
    id              SERIAL PRIMARY KEY,
    ticket_id       INTEGER REFERENCES tickets(id) ON DELETE CASCADE,
    manager_id      INTEGER REFERENCES managers(id),
    assignment_rule TEXT,
    status          TEXT DEFAULT 'new',
    assigned_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickets_segment    ON tickets(segment);
CREATE INDEX IF NOT EXISTS idx_managers_bu        ON managers(business_unit_id);
CREATE INDEX IF NOT EXISTS idx_assignments_ticket ON assignments(ticket_id);

CREATE OR REPLACE VIEW v_full_assignment AS
SELECT
    t.id              AS ticket_id,
    t.client_guid,
    t.segment,
    t.city            AS client_city,
    t.description,
    ai.request_type,
    ai.tone,
    ai.priority_score,
    ai.language,
    ai.summary,
    m.full_name       AS manager_name,
    m.position        AS manager_position,
    bu.office_name    AS manager_office,
    a.status,
    a.assigned_at
FROM assignments a
JOIN tickets         t  ON t.id = a.ticket_id
JOIN managers        m  ON m.id = a.manager_id
LEFT JOIN ai_analysis ai ON ai.ticket_id = t.id
LEFT JOIN business_units bu ON bu.id = m.business_unit_id;
"""


# ══════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════════════

def clean(value: str) -> str | None:
    """Убираем лишние пробелы и переводим пустую строку в None."""
    if value is None:
        return None
    v = value.strip()
    return v if v else None


def parse_date(value: str) -> str | None:
    """Пробуем распарсить дату в разных форматах."""
    if not value or not value.strip():
        return None
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d 0:00", "%Y-%m-%d", "%d.%m.%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    print(f"  ⚠️  Не удалось распарсить дату: '{value}' — записано как NULL")
    return None


def parse_skills(value: str) -> list:
    """'VIP, ENG, KZ' → ['VIP', 'ENG', 'KZ']"""
    if not value or not value.strip():
        return []
    return [s.strip().upper() for s in re.split(r"[,;]", value) if s.strip()]


def read_csv(filepath: str) -> tuple[list[str], list[list[str]]]:
    """Читаем CSV и возвращаем (заголовки, строки)."""
    with open(filepath, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = [h.strip() for h in next(reader)]
        rows = [row for row in reader if any(cell.strip() for cell in row)]
    return headers, rows


def row_to_dict(headers: list, row: list) -> dict:
    """Превращаем строку CSV в словарь по заголовкам."""
    return {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}


# ══════════════════════════════════════════════════════════════
#  ПАРСЕРЫ ДЛЯ КАЖДОГО ФАЙЛА
# ══════════════════════════════════════════════════════════════

def parse_business_units(filepath: str) -> list[dict]:
    """
    Парсим business_units.csv
    Колонки: Офис, Адрес
    """
    print(f"\n📂 Читаю файл: {filepath}")
    headers, rows = read_csv(filepath)
    print(f"   Заголовки: {headers}")
    print(f"   Строк данных: {len(rows)}")

    result = []
    for i, row in enumerate(rows, 1):
        d = row_to_dict(headers, row)
        record = {
            "office_name": clean(d.get("Офис") or d.get("Office") or d.get("office")),
            "address":     clean(d.get("Адрес") or d.get("Address") or d.get("address")),
        }
        if not record["office_name"]:
            print(f"   ⚠️  Строка {i}: пустое название офиса — пропускаем")
            continue
        result.append(record)
        print(f"   ✔  [{i}] Офис: {record['office_name']} | Адрес: {record['address'][:40] if record['address'] else '—'}...")

    print(f"   Итого распарсено: {len(result)} записей")
    return result


def parse_managers(filepath: str) -> list[dict]:
    """
    Парсим managers.csv
    Колонки: ФИО, Должность, Офис, Навыки, Количество обращений в работе
    """
    print(f"\n📂 Читаю файл: {filepath}")
    headers, rows = read_csv(filepath)
    print(f"   Заголовки: {headers}")
    print(f"   Строк данных: {len(rows)}")

    result = []
    for i, row in enumerate(rows, 1):
        d = row_to_dict(headers, row)

        skills_raw = clean(d.get("Навыки") or d.get("Skills") or d.get("skills") or "")
        active_raw = clean(d.get("Количество обращений в работе") or d.get("active_tickets") or "0")

        record = {
            "full_name":      clean(d.get("ФИО") or d.get("full_name") or d.get("Name")),
            "position":       clean(d.get("Должность ") or d.get("Должность") or d.get("position") or d.get("Position")),
            "office_name":    clean(d.get("Офис") or d.get("office") or d.get("Office")),  # временно — потом заменим на FK
            "skills":         parse_skills(skills_raw),
            "active_tickets": int(active_raw) if active_raw and active_raw.isdigit() else 0,
        }

        if not record["full_name"]:
            print(f"   ⚠️  Строка {i}: пустое ФИО — пропускаем")
            continue

        result.append(record)
        print(f"   ✔  [{i}] {record['full_name']} | {record['position']} | Офис: {record['office_name']} | Навыки: {record['skills']} | Нагрузка: {record['active_tickets']}")

    print(f"   Итого распарсено: {len(result)} записей")
    return result


def parse_tickets(filepath: str) -> list[dict]:
    """
    Парсим tickets.csv
    Колонки: GUID клиента, Пол клиента, Дата рождения, Описание, Вложения,
             Сегмент клиента, Страна, Область, Населённый пункт, Улица, Дом
    """
    print(f"\n📂 Читаю файл: {filepath}")
    headers, rows = read_csv(filepath)
    print(f"   Заголовки: {headers}")
    print(f"   Строк данных: {len(rows)}")

    result = []
    for i, row in enumerate(rows, 1):
        d = row_to_dict(headers, row)

        record = {
            "client_guid": clean(d.get("GUID клиента") or d.get("client_guid") or d.get("GUID")),
            "gender":      clean(d.get("Пол клиента") or d.get("gender") or d.get("Пол")),
            "birth_date":  parse_date(d.get("Дата рождения") or d.get("birth_date") or ""),
            "description": clean(d.get("Описание ") or d.get("Описание") or d.get("description")),
            "attachment":  clean(d.get("Вложения") or d.get("attachment")),
            "segment":     clean(d.get("Сегмент клиента") or d.get("Сегмент") or d.get("segment")),
            "country":     clean(d.get("Страна") or d.get("country")),
            "region":      clean(d.get("Область") or d.get("region")),
            "city":        clean(d.get("Населённый пункт") or d.get("Населенный пункт") or d.get("city")),
            "street":      clean(d.get("Улица") or d.get("street")),
            "building":    clean(d.get("Дом") or d.get("building")),
        }

        if not record["client_guid"]:
            print(f"   ⚠️  Строка {i}: пустой GUID — пропускаем")
            continue

        desc_preview = (record["description"] or "")[:50].replace("\n", " ")
        print(f"   ✔  [{i}] GUID: {record['client_guid'][:8]}... | Сегмент: {record['segment']} | Город: {record['city']} | Описание: {desc_preview}...")

    result.append(record)

    print(f"   Итого распарсено: {len(result)} записей")
    return result


# Исправленная версия parse_tickets с правильным append
def parse_tickets(filepath: str) -> list[dict]:
    print(f"\n📂 Читаю файл: {filepath}")
    headers, rows = read_csv(filepath)
    print(f"   Заголовки: {headers}")
    print(f"   Строк данных: {len(rows)}")

    result = []
    for i, row in enumerate(rows, 1):
        d = row_to_dict(headers, row)

        record = {
            "client_guid": clean(d.get("GUID клиента") or d.get("client_guid") or d.get("GUID")),
            "gender":      clean(d.get("Пол клиента") or d.get("gender") or d.get("Пол")),
            "birth_date":  parse_date(d.get("Дата рождения") or d.get("birth_date") or ""),
            "description": clean(d.get("Описание ") or d.get("Описание") or d.get("description")),
            "attachment":  clean(d.get("Вложения") or d.get("attachment")),
            "segment":     clean(d.get("Сегмент клиента") or d.get("Сегмент") or d.get("segment")),
            "country":     clean(d.get("Страна") or d.get("country")),
            "region":      clean(d.get("Область") or d.get("region")),
            "city":        clean(d.get("Населённый пункт") or d.get("Населенный пункт") or d.get("city")),
            "street":      clean(d.get("Улица") or d.get("street")),
            "building":    clean(d.get("Дом") or d.get("building")),
        }

        if not record["client_guid"]:
            print(f"   ⚠️  Строка {i}: пустой GUID — пропускаем")
            continue

        result.append(record)
        desc_preview = (record["description"] or "")[:50].replace("\n", " ")
        print(f"   ✔  [{i}] GUID: {record['client_guid'][:8]}... | Сегмент: {record['segment']} | Город: {record['city']} | Описание: {desc_preview}...")

    print(f"   Итого распарсено: {len(result)} записей")
    return result


# ══════════════════════════════════════════════════════════════
#  СОХРАНЕНИЕ В POSTGRESQL
# ══════════════════════════════════════════════════════════════

def create_db_if_not_exists(cfg: dict):
    """Создаём БД если не существует."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from psycopg2 import sql

    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"],
                             user=cfg["user"], password=cfg["password"], dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg["dbname"],))
    if not cur.fetchone():
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cfg["dbname"])))
        print(f"\n✅ База данных '{cfg['dbname']}' создана.")
    else:
        print(f"\nℹ️  База данных '{cfg['dbname']}' уже существует.")
    cur.close()
    conn.close()


def apply_schema(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("✅ Схема БД применена.")


def save_business_units(conn, records: list[dict]) -> dict:
    """Сохраняем офисы и возвращаем маппинг {office_name: id}."""
    print(f"\n💾 Сохраняю business_units ({len(records)} записей)...")
    office_map = {}
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO business_units (office_name, address)
                VALUES (%s, %s)
                ON CONFLICT (office_name) DO UPDATE SET address = EXCLUDED.address
                RETURNING id
            """, (r["office_name"], r["address"]))
            office_id = cur.fetchone()[0]
            office_map[r["office_name"]] = office_id
    conn.commit()
    print(f"   ✅ Сохранено: {len(records)} офисов")
    return office_map


def save_managers(conn, records: list[dict], office_map: dict):
    """Сохраняем менеджеров, подставляя FK на офис."""
    print(f"\n💾 Сохраняю managers ({len(records)} записей)...")
    saved = 0
    with conn.cursor() as cur:
        for r in records:
            office_name = r.get("office_name")
            bu_id = office_map.get(office_name)
            if not bu_id and office_name:
                print(f"   ⚠️  Офис '{office_name}' не найден в БД — manager '{r['full_name']}' будет без офиса")

            cur.execute("""
                INSERT INTO managers (full_name, position, business_unit_id, skills, active_tickets)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                r["full_name"],
                r["position"],
                bu_id,
                r["skills"],
                r["active_tickets"],
            ))
            saved += 1
    conn.commit()
    print(f"   ✅ Сохранено: {saved} менеджеров")


def save_tickets(conn, records: list[dict]):
    """Сохраняем тикеты."""
    print(f"\n💾 Сохраняю tickets ({len(records)} записей)...")
    saved = 0
    with conn.cursor() as cur:
        for r in records:
            cur.execute("""
                INSERT INTO tickets (client_guid, gender, birth_date, description,
                                     attachment, segment, country, region, city, street, building)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r["client_guid"],
                r["gender"],
                r["birth_date"],
                r["description"],
                r["attachment"],
                r["segment"],
                r["country"],
                r["region"],
                r["city"],
                r["street"],
                r["building"],
            ))
            saved += 1
    conn.commit()
    print(f"   ✅ Сохранено: {saved} тикетов")


# ══════════════════════════════════════════════════════════════
#  СТАТИСТИКА
# ══════════════════════════════════════════════════════════════

def print_stats(conn):
    print("\n" + "═" * 50)
    print("📊 ИТОГОВАЯ СТАТИСТИКА В БД:")
    print("═" * 50)
    with conn.cursor() as cur:
        for table in ["business_units", "managers", "tickets", "ai_analysis", "assignments"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"   {table:<22} → {count:>4} строк")
    print("═" * 50)


# ══════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(description="CSV Parser → PostgreSQL")
    parser.add_argument("--tickets",        default=CSV_FILES["tickets"])
    parser.add_argument("--managers",       default=CSV_FILES["managers"])
    parser.add_argument("--business_units", default=CSV_FILES["business_units"])
    parser.add_argument("--host",     default=DEFAULT_CONFIG["host"])
    parser.add_argument("--port",     default=DEFAULT_CONFIG["port"], type=int)
    parser.add_argument("--user",     default=DEFAULT_CONFIG["user"])
    parser.add_argument("--password", default=DEFAULT_CONFIG["password"])
    parser.add_argument("--dbname",   default=DEFAULT_CONFIG["dbname"])
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = {"host": args.host, "port": args.port, "user": args.user,
           "password": args.password, "dbname": args.dbname}

    print("╔══════════════════════════════════════════╗")
    print("║     CSV → PostgreSQL Parser              ║")
    print("╚══════════════════════════════════════════╝")
    print(f"Подключение: {cfg['user']}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}")

    # ── ШАГИ ПАРСИНГА ──────────────────────────────────────────

    # ШАГ 1: Читаем все три файла
    print("\n" + "─" * 50)
    print("ШАГ 1: ПАРСИНГ CSV ФАЙЛОВ")
    print("─" * 50)
    bu_records      = parse_business_units(args.business_units)
    manager_records = parse_managers(args.managers)
    ticket_records  = parse_tickets(args.tickets)

    # ШАГ 2: Подключаемся к PostgreSQL
    print("\n" + "─" * 50)
    print("ШАГ 2: ПОДКЛЮЧЕНИЕ К POSTGRESQL")
    print("─" * 50)
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2 не установлен. Выполните:")
        print("   pip install psycopg2-binary")
        sys.exit(1)

    create_db_if_not_exists(cfg)
    conn = psycopg2.connect(**cfg)
    print(f"✅ Подключение успешно.")

    try:
        # ШАГ 3: Создаём схему
        print("\n" + "─" * 50)
        print("ШАГ 3: СОЗДАНИЕ СХЕМЫ БД")
        print("─" * 50)
        apply_schema(conn)

        # ШАГ 4: Сохраняем данные последовательно
        print("\n" + "─" * 50)
        print("ШАГ 4: СОХРАНЕНИЕ ДАННЫХ")
        print("─" * 50)
        office_map = save_business_units(conn, bu_records)   # 1-й: офисы
        save_managers(conn, manager_records, office_map)     # 2-й: менеджеры (нужен FK офисов)
        save_tickets(conn, ticket_records)                   # 3-й: тикеты

        # ШАГ 5: Итоговая статистика
        print_stats(conn)

        print("\n✅ Всё готово! База данных заполнена.")
        print(f"\n💡 Проверьте результат:")
        print(f"   psql -h {cfg['host']} -U {cfg['user']} -d {cfg['dbname']}")
        print(f"   SELECT * FROM v_full_assignment LIMIT 10;")
        print(f"   SELECT office_name, COUNT(*) FROM managers JOIN business_units bu ON bu.id = business_unit_id GROUP BY 1;")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Ошибка при сохранении: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()