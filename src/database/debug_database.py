import sqlite3
import os

def debug_database():
    """Диагностика базы данных"""
    db_path = "industrial_vacancies.db"
    
    print(f"ДИАГНОСТИКА БАЗЫ ДАННЫХ")
    print("=" * 60)
    
    # Проверяем существование файла
    if not os.path.exists(db_path):
        print(f"[X] Файл базы данных не найден: {db_path}")
        return
        
    file_size = os.path.getsize(db_path)
    print(f"[X] Файл базы найден, размер: {file_size} байт")
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Показываем ВСЕ таблицы
        cursor.execute("SELECT name, type FROM sqlite_master")
        all_objects = cursor.fetchall()
        
        print(f"\nВСЕ ОБЪЕКТЫ В БАЗЕ ({len(all_objects)}):")
        for obj in all_objects:
            print(f"  - {obj['name']} ({obj['type']})")
        
        # Проверяем структуру таблицы vacancies
        print(f"\nСТРУКТУРА ТАБЛИЦЫ vacancies:")
        try:
            cursor.execute("PRAGMA table_info(vacancies)")
            columns = cursor.fetchall()
            if columns:
                for col in columns:
                    print(f"  - {col['name']} ({col['type']})")
            else:
                print("  [X] Таблица vacancies не существует или пуста")
        except:
            print("  [X] Не удалось прочитать структуру vacancies")
        
        # Считаем количество записей
        print(f"\nСТАТИСТИКА:")
        try:
            cursor.execute("SELECT COUNT(*) as count FROM vacancies")
            vac_count = cursor.fetchone()['count']
            print(f"  - Вакансий: {vac_count}")
            
            cursor.execute("SELECT COUNT(*) as count FROM skills")
            skills_count = cursor.fetchone()['count']
            print(f"  - Навыков: {skills_count}")
        except:
            print("  [X] Не удалось получить статистику")
        
        # Показываем ВСЕ данные из vacancies
        print(f"\n ВСЕ ДАННЫЕ ИЗ vacancies:")
        try:
            cursor.execute("SELECT * FROM vacancies")
            vacancies = cursor.fetchall()
            
            if vacancies:
                for i, vacancy in enumerate(vacancies, 1):
                    print(f"  {i}. ID: {vacancy['id']}")
                    print(f"     Название: {vacancy['name']}")
                    print(f"     Компания: {vacancy['employer_name']}")
                    print(f"     Регион: {vacancy['area']}")
            else:
                print("  [!] Таблица vacancies пуста")
        except Exception as e:
            print(f"  [X] Ошибка при чтении vacancies: {e}")
        
        conn.close()
        print(f"\n[V] Диагностика завершена")
        
    except Exception as e:
        print(f"[X] Ошибка подключения к базе: {e}")

if __name__ == "__main__":
    debug_database()