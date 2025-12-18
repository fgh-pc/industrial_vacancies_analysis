"""
Скрипт для отображения вида датасета (примеры данных)
Выводит структурированное представление данных из базы
"""

import sqlite3
import os
from datetime import datetime

def print_header(title, width=100):
    """Печатает заголовок"""
    print("\n" + "="*width)
    print(f"  {title}")
    print("="*width)


def print_separator(width=100):
    """Печатает разделитель"""
    print("-"*width)


def display_database_schema_view():
    """Отображает структуру базы данных"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных не найдена: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Получаем статистику
    cursor.execute("SELECT COUNT(*) as count FROM vacancies WHERE is_industrial = 1")
    total_vacancies = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(DISTINCT region) as count FROM vacancies WHERE region IS NOT NULL")
    unique_regions = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(DISTINCT employer_name) as count FROM vacancies WHERE employer_name IS NOT NULL")
    unique_employers = cursor.fetchone()['count']
    
    file_size = os.path.getsize(db_path)
    
    print_header("СТРУКТУРА БАЗЫ ДАННЫХ", 100)
    print(f"\n📊 База данных: industrial_vacancies.db")
    print(f"📁 Размер файла: {file_size:,} байт ({file_size / 1024 / 1024:.2f} MB)")
    print(f"\n📈 Статистика:")
    print(f"   • Всего промышленных вакансий: {total_vacancies:,}")
    print(f"   • Уникальных регионов: {unique_regions}")
    print(f"   • Уникальных работодателей: {unique_employers:,}")
    
    # Показываем таблицы
    print(f"\n📋 Таблицы в базе данных:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table['name']
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        count = cursor.fetchone()['count']
        print(f"   • {table_name:25} - {count:>12,} записей")
    
    conn.close()


def display_vacancies_sample(limit=5):
    """Отображает примеры вакансий в читаемом виде"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print_header("ПРИМЕРЫ ДАННЫХ: ТАБЛИЦА VACANCIES", 100)
    
    cursor.execute(f"""
        SELECT 
            id, hh_id, name, employer_name, region, 
            salary_from, salary_to, salary_avg_rub, salary_currency,
            industry_segment, position_level, experience,
            schedule, employment,
            published_at, is_industrial, has_salary
        FROM vacancies 
        WHERE is_industrial = 1 
        AND has_salary = 1
        LIMIT {limit}
    """)
    
    rows = cursor.fetchall()
    
    for i, row in enumerate(rows, 1):
        print(f"\n{'─'*100}")
        print(f"📌 ВАКАНСИЯ #{i} (ID: {row['id']})")
        print(f"{'─'*100}")
        
        print(f"\n📝 Основная информация:")
        print(f"   ID HeadHunter:     {row['hh_id']}")
        print(f"   Название:          {row['name']}")
        
        print(f"\n🏢 Работодатель:")
        print(f"   Компания:          {row['employer_name']}")
        
        print(f"\n📍 Локация:")
        print(f"   Регион:            {row['region']}")
        
        print(f"\n💰 Зарплата:")
        if row['salary_from'] or row['salary_to']:
            salary_str = f"{row['salary_from']:,}" if row['salary_from'] else "не указано"
            salary_str += f" - {row['salary_to']:,}" if row['salary_to'] else ""
            salary_str += f" {row['salary_currency']}" if row['salary_currency'] else ""
            print(f"   Диапазон:          {salary_str}")
        if row['salary_avg_rub']:
            print(f"   Средняя (руб):     {row['salary_avg_rub']:,} руб")
        else:
            print(f"   Средняя (руб):     не указана")
        
        print(f"\n🏭 Классификация:")
        print(f"   Отраслевой сегмент: {row['industry_segment'] or 'не указан'}")
        print(f"   Уровень позиции:    {row['position_level'] or 'не указан'}")
        
        print(f"\n👔 Условия:")
        print(f"   Опыт работы:        {row['experience'] or 'не указан'}")
        print(f"   График работы:      {row['schedule'] or 'не указан'}")
        print(f"   Тип занятости:      {row['employment'] or 'не указан'}")
        
        print(f"\n📅 Временные метки:")
        print(f"   Дата публикации:    {row['published_at'] or 'не указана'}")
        
        print(f"\n✅ Флаги:")
        print(f"   Промышленная:       {'Да' if row['is_industrial'] else 'Нет'}")
        print(f"   Есть зарплата:      {'Да' if row['has_salary'] else 'Нет'}")
    
    print(f"\n{'─'*100}\n")
    
    conn.close()


def display_skills_sample(limit=15):
    """Отображает примеры навыков"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print_header("ПРИМЕРЫ ДАННЫХ: ТАБЛИЦА SKILLS", 100)
    
    # Показываем топ навыков
    cursor.execute(f"""
        SELECT 
            skill_name,
            COUNT(*) as frequency,
            COUNT(DISTINCT vacancy_id) as vacancy_count
        FROM skills
        GROUP BY skill_name
        ORDER BY frequency DESC
        LIMIT {limit}
    """)
    
    rows = cursor.fetchall()
    
    print(f"\n{'─'*100}")
    print(f"{'№':<4} {'Навык':<50} {'Частота':<15} {'Вакансий':<15}")
    print(f"{'─'*100}")
    
    for i, row in enumerate(rows, 1):
        skill_name = row['skill_name'][:48] + '..' if len(row['skill_name']) > 50 else row['skill_name']
        print(f"{i:<4} {skill_name:<50} {row['frequency']:<15,} {row['vacancy_count']:<15,}")
    
    print(f"{'─'*100}\n")
    
    # Показываем примеры связей навыков с вакансиями
    print(f"\n📌 Примеры связей навыков с вакансиями:")
    print(f"{'─'*100}")
    
    cursor.execute(f"""
        SELECT 
            s.skill_name,
            v.name as vacancy_name,
            v.industry_segment
        FROM skills s
        JOIN vacancies v ON s.vacancy_id = v.id
        WHERE v.is_industrial = 1
        LIMIT 10
    """)
    
    rows = cursor.fetchall()
    
    for i, row in enumerate(rows, 1):
        vacancy_name = row['vacancy_name'][:60] + '..' if len(row['vacancy_name']) > 62 else row['vacancy_name']
        print(f"{i:2}. Навык: {row['skill_name']:30} → Вакансия: {vacancy_name}")
        if row['industry_segment']:
            print(f"    Сегмент: {row['industry_segment']}")
    
    print(f"{'─'*100}\n")
    
    conn.close()


def display_dataset_summary():
    """Отображает сводку по датасету"""
    print_header("ХАРАКТЕРИСТИКИ ДАТАСЕТА", 100)
    
    summary = """
📊 ОСНОВНЫЕ ХАРАКТЕРИСТИКИ:
   • Анализируемый период:        2 месяца
   • Еженедельный запрос к API:   ~5,000 запросов
   • Всего запросов к API:        ~50,000
   • Еженедельный сбор:           ~300,000 вакансий
   • Всего собрано:               ~3,000,000 вакансий
   • Отфильтровано:               ~2,781,212
   • Объем БД:                    1,179,419 записей
   • Уникальных вакансий:         218,788
   • Время сбора (за неделю):     23 минуты

📋 СТРУКТУРА БАЗЫ ДАННЫХ:
   База данных: SQLite (industrial_vacancies.db)
   
   Таблицы:
   1. vacancies          - Основная таблица вакансий
   2. skills             - Нормализованная таблица навыков
   3. regions            - Региональная аналитика
   4. industry_segments  - Отраслевые сегменты
   5. time_series        - Временные ряды

🔗 ОСНОВНЫЕ СВЯЗИ:
   vacancies (1) ←→ (Many) skills
      └── vacancy_id → vacancies.id
"""
    
    print(summary)
    print("="*100)


def main():
    """Основная функция"""
    print("\n" + "█"*100)
    print("█" + " "*98 + "█")
    print("█" + "ВИД ДАТАСЕТА: АНАЛИЗ ПРОМЫШЛЕННЫХ ВАКАНСИЙ".center(98) + "█")
    print("█" + f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(98) + "█")
    print("█" + " "*98 + "█")
    print("█"*100)
    
    display_dataset_summary()
    display_database_schema_view()
    display_vacancies_sample(5)
    display_skills_sample(15)
    
    print("\n" + "█"*100)
    print("█" + "КОНЕЦ ПРЕДСТАВЛЕНИЯ ДАТАСЕТА".center(98) + "█")
    print("█"*100 + "\n")
    
    print("\n💡 Для создания скриншота:")
    print("   1. Запустите этот скрипт: python scripts/display_dataset_view.py")
    print("   2. Скопируйте вывод или сделайте скриншот терминала")
    print("   3. Для сохранения в файл используйте: python scripts/display_dataset_view.py > dataset_view.txt\n")


if __name__ == "__main__":
    main()

