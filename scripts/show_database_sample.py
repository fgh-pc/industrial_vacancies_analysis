"""
Показывает примеры данных из базы данных
"""

import sqlite3
import os
import json

def show_vacancies_sample(limit=3):
    """Показывает примеры вакансий"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("="*80)
    print("ПРИМЕРЫ ДАННЫХ ИЗ ТАБЛИЦЫ VACANCIES")
    print("="*80)
    
    cursor.execute(f"""
        SELECT 
            id, hh_id, name, employer_name, region, 
            salary_avg_rub, industry_segment, position_level,
            published_at, is_industrial, has_salary
        FROM vacancies 
        WHERE is_industrial = 1 
        LIMIT {limit}
    """)
    
    rows = cursor.fetchall()
    
    for i, row in enumerate(rows, 1):
        print(f"\n{'─'*80}")
        print(f"ВАКАНСИЯ #{i}")
        print(f"{'─'*80}")
        print(f"ID: {row['id']}")
        print(f"HH ID: {row['hh_id']}")
        print(f"Название: {row['name']}")
        print(f"Работодатель: {row['employer_name']}")
        print(f"Регион: {row['region']}")
        print(f"Зарплата: {row['salary_avg_rub']:,} руб" if row['salary_avg_rub'] else "Зарплата: не указана")
        print(f"Отраслевой сегмент: {row['industry_segment']}")
        print(f"Уровень позиции: {row['position_level']}")
        print(f"Дата публикации: {row['published_at']}")
        print(f"Промышленная: {'Да' if row['is_industrial'] else 'Нет'}")
        print(f"Есть зарплата: {'Да' if row['has_salary'] else 'Нет'}")
    
    conn.close()
    print(f"\n{'─'*80}\n")


def show_skills_sample(limit=10):
    """Показывает примеры навыков"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("="*80)
    print("ПРИМЕРЫ ДАННЫХ ИЗ ТАБЛИЦЫ SKILLS")
    print("="*80)
    
    cursor.execute(f"""
        SELECT s.*, v.name as vacancy_name
        FROM skills s
        JOIN vacancies v ON s.vacancy_id = v.id
        LIMIT {limit}
    """)
    
    rows = cursor.fetchall()
    
    for i, row in enumerate(rows, 1):
        print(f"\n{i}. Навык: {row['skill_name']}")
        print(f"   Вакансия: {row['vacancy_name'][:60]}...")
        print(f"   Категория: {row['skill_category'] or 'не указана'}")
    
    conn.close()
    print(f"\n{'─'*80}\n")


def show_database_stats():
    """Показывает статистику базы данных"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("="*80)
    print("СТАТИСТИКА БАЗЫ ДАННЫХ")
    print("="*80)
    
    # Количество записей по таблицам
    tables = ['vacancies', 'skills', 'regions', 'industry_segments', 'time_series']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:25} {count:>12,} записей")
        except:
            print(f"{table:25} {'таблица не найдена':>12}")
    
    # Дополнительная статистика по вакансиям
    print("\n" + "─"*80)
    print("СТАТИСТИКА ПО ВАКАНСИЯМ:")
    print("─"*80)
    
    cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_industrial = 1")
    industrial = cursor.fetchone()[0]
    print(f"Промышленных вакансий: {industrial:,}")
    
    cursor.execute("SELECT COUNT(*) FROM vacancies WHERE has_salary = 1")
    with_salary = cursor.fetchone()[0]
    print(f"С указанной зарплатой: {with_salary:,}")
    
    cursor.execute("SELECT COUNT(DISTINCT region) FROM vacancies WHERE region IS NOT NULL")
    regions = cursor.fetchone()[0]
    print(f"Уникальных регионов: {regions}")
    
    cursor.execute("SELECT COUNT(DISTINCT employer_name) FROM vacancies WHERE employer_name IS NOT NULL")
    employers = cursor.fetchone()[0]
    print(f"Уникальных работодателей: {employers:,}")
    
    file_size = os.path.getsize(db_path)
    print(f"\nРазмер файла БД: {file_size:,} байт ({file_size / 1024 / 1024:.2f} MB)")
    
    conn.close()
    print("─"*80 + "\n")


if __name__ == "__main__":
    show_database_stats()
    show_vacancies_sample(3)
    show_skills_sample(10)

