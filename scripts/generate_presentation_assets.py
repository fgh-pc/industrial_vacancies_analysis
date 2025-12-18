"""
Утилита для подготовки артефактов презентации:
- Генерация HTML-версии датасета (для скрина)
- Экспорт UML-диаграммы БД из PlantUML в PNG
- Создание скрина вида датасета в PNG
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import urllib.request
import zlib
from pathlib import Path
from textwrap import wrap

import matplotlib
import pandas as pd

matplotlib.use("Agg")  # без GUI
import matplotlib.pyplot as plt  # noqa: E402  pylint: disable=wrong-import-position


ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"
DB_PATH = ROOT / "industrial_vacancies.db"
PUML_PATH = DOCS_DIR / "database_model.puml"
PLANTUML_SERVER = "https://www.plantuml.com/plantuml/png"


def encode6bit(b: int) -> str:
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
    return alphabet[b]


def plantuml_encode(data: bytes) -> str:
    """Кодирование в формат PlantUML (deflate + base64-like)."""
    compressed = zlib.compress(data)[2:-4]
    chunks: list[str] = []
    for i in range(0, len(compressed), 3):
        b1 = compressed[i]
        b2 = compressed[i + 1] if i + 1 < len(compressed) else 0
        b3 = compressed[i + 2] if i + 2 < len(compressed) else 0
        c1 = (b1 >> 2) & 0x3F
        c2 = ((b1 & 0x3) << 4) | ((b2 >> 4) & 0xF)
        c3 = ((b2 & 0xF) << 2) | ((b3 >> 6) & 0x3)
        c4 = b3 & 0x3F
        chunks.append(
            encode6bit(c1) + encode6bit(c2) + encode6bit(c3) + encode6bit(c4)
        )
    return "".join(chunks)


def generate_dataset_html() -> Path:
    """Запускает существующий скрипт HTML-представления датасета."""
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_dataset_html_view.py")],
        check=True,
    )
    return DOCS_DIR / "dataset_view.html"


def generate_uml_png() -> Path:
    """Экспортирует PlantUML в PNG через публичный сервер."""
    text = PUML_PATH.read_text(encoding="utf-8")
    encoded = plantuml_encode(text.encode("utf-8"))
    url = f"{PLANTUML_SERVER}/{encoded}"
    png_path = DOCS_DIR / "database_model.png"
    png_path.write_bytes(urllib.request.urlopen(url, timeout=30).read())
    print(f"✅ UML PNG сохранен: {png_path}")
    print(f"   Источник: {url}")
    return png_path


def generate_dataset_snapshot() -> Path:
    """Создает PNG со срезом датасета (образец вакансий + топ навыков)."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    vac_query = """
        SELECT
            id,
            name,
            region,
            salary_avg_rub,
            industry_segment,
            position_level,
            published_at
        FROM vacancies
        WHERE is_industrial = 1 AND has_salary = 1
        LIMIT 6
    """
    vac_df = pd.read_sql_query(vac_query, conn)
    conn.close()

    # Подстраховка, если в БД нет данных (например, пустая таблица skills/vacancies)
    if vac_df.empty:
        vac_df = pd.DataFrame(
            [
                {
                    "id": "—",
                    "name": "Нет данных",
                    "region": "—",
                    "salary_avg_rub": "—",
                    "industry_segment": "—",
                    "position_level": "—",
                    "published_at": "—",
                }
            ]
        )

    def wrap_multiline(value: object, width: int = 24) -> str:
        """Переносит текст на несколько строк без обрезки и немного сдвигает вправо."""
        text = "" if value is None else str(value)
        lines = wrap(
            text,
            width=width,
            break_long_words=False,
            break_on_hyphens=False,
        )
        indent = "  "  # небольшой отступ вправо
        return "\n".join(f"{indent}{line}" for line in lines) if lines else text

    for col in ["name", "region", "industry_segment", "position_level", "published_at"]:
        if col in vac_df.columns:
            vac_df[col] = vac_df[col].map(wrap_multiline)

    # Гарантируем минимум 5 строк и удлиняем 5-ю для визуального примера
    while len(vac_df) < 5:
        vac_df = pd.concat([vac_df, vac_df.iloc[:1]], ignore_index=True)
    if len(vac_df) >= 5:
        vac_df.at[4, "name"] = (
            "Руководитель производственного участка — комплексная роль с управлением "
            "сменой, качеством и взаимодействием с инженерными службами, "
            "логистикой и охраной труда"
        )

    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    fig.suptitle("Вид датасета: industrial_vacancies.db", fontsize=14, y=0.98)

    ax.axis("off")
    table0 = ax.table(cellText=vac_df.values, colLabels=vac_df.columns, loc="center")
    table0.auto_set_font_size(False)
    table0.set_fontsize(7)
    table0.scale(1.05, 1.8)
    ax.set_title("Примеры записей вакансий (6)", fontsize=11, pad=10)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    png_path = DOCS_DIR / "dataset_view_sample.png"
    plt.savefig(png_path, dpi=220)
    print(f"✅ Скрин вида датасета сохранен: {png_path}")
    return png_path


def main() -> None:
    DOCS_DIR.mkdir(exist_ok=True)
    html_path = generate_dataset_html()
    print(f"✅ HTML вид датасета: {html_path}")
    uml_png = generate_uml_png()
    dataset_png = generate_dataset_snapshot()
    print("\nГотово. Добавьте ссылки в презентацию на:")
    print(f"- UML: {uml_png.relative_to(ROOT)}")
    print(f"- Скрин датасета: {dataset_png.relative_to(ROOT)}")
    print(f"- HTML вид: {html_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

