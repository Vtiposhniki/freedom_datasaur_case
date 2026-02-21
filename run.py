import os
import subprocess
import pandas as pd
from engine import FIRE_Engine_V10

REQUIRED_FILES = [
    "./dataset/extracted/tickets.csv",
    "./dataset/extracted/managers.csv",
    "./dataset/extracted/business_units.csv"
]


def ensure_data():
    missing = [f for f in REQUIRED_FILES if not os.path.exists(f)]

    if not missing:
        return

    print("Missing files:", ", ".join(missing))
    print("Running downloader...")

    # запуск download_files.py
    result = subprocess.run(
        ["python", "download_files.py"],
        capture_output=True,
        text=True
    )

    print(result.stdout)
    print(result.stderr)

    # повторная проверка
    still_missing = [f for f in REQUIRED_FILES if not os.path.exists(f)]

    if still_missing:
        raise RuntimeError(
            f"Files still missing after download: {still_missing}"
        )


def main():

    ensure_data()

    tickets = pd.read_csv(REQUIRED_FILES[0])
    managers = pd.read_csv(REQUIRED_FILES[1])
    units = pd.read_csv(REQUIRED_FILES[2])

    engine = FIRE_Engine_V10(
        tickets,
        managers,
        units,
        enable_fallback=False
    )

    out = engine.distribute()

    print(out.head(10))
    os.makedirs("dataset/processed", exist_ok=True)
    out.to_csv("./dataset/processed/assignments.csv", index=False, encoding="utf-8-sig")

    print("saved: dataset/processed/assignments.csv")


if __name__ == "__main__":
    main()