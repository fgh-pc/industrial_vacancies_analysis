import sqlite3
import os

def view_database():
    """Просмотр содержимого базы данных"""
    db_path = "industrial_vacancies.db"
    
    if not os.path.exists(db_path):
        print(f"[X] База данных не найдена!")
        return
        
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print(" СОДЕРЖИМОЕ БАЗЫ ДАННЫХ")
    print("=" * 50)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    print(" ТАБЛИЦЫ В БАЗЕ:")
    for table in tables:
        print(f"  - {table['name']}")
    
    print("\n" + "=" * 50)

    print(" ВАКАНСИИ:")
    cursor.execute("SELECT * FROM vacancies")
    vacancies = cursor.fetchall()
    
    for vacancy in vacancies:
        print(f"\nID: {vacancy['id']}")
        print(f"Название: {vacancy['name']}")
        print(f"Компания: {vacancy['employer_name']}")
        print(f"Зарплата: {vacancy['salary_from']} - {vacancy['salary_to']} {vacancy['salary_currency']}")
        print(f"Регион: {vacancy['area']}")
    
    print("\n" + "=" * 50)
    
    print(" НАВЫКИ:")
    cursor.execute("""
        SELECT v.name as vacancy_name, s.skill_name 
        FROM skills s 
        JOIN vacancies v ON s.vacancy_id = v.id
    """)
    skills = cursor.fetchall()
    
    for skill in skills:
        print(f"Вакансия: {skill['vacancy_name']} -> Навык: {skill['skill_name']}")
    
    conn.close()

if __name__ == "__main__":
    view_database()