import sqlite3
import os
from typing import Dict, List, Optional

class DatabaseManager:

    
    def __init__(self, db_path: str = "industrial_vacancies.db"):
        """
        Инициализация менеджера базы данных.
        
        Аргументы:
            db_path (str): Путь к файлу базы данных
        """
        self.db_path = db_path
        self.connection = None
        
    def create_connection(self) -> bool:
        """
        Создает соединение с SQLite базой данных.
        
        Возвращает:
            bool: True если подключение успешно, False если ошибка
        """
        try:
            
            self.connection = sqlite3.connect(self.db_path)
           
            self.connection.row_factory = sqlite3.Row
            print(f" Успешное подключение к базе данных: {self.db_path}")
            return True
            
        except sqlite3.Error as e:
            print(f" Ошибка подключения к базе данных: {e}")
            return False
            
    def create_tables(self) -> bool:
        """
        Создает таблицы в базе данных из SQL файла.
        
        Возвращает:
            bool: True если таблицы созданы успешно
        """
        try:
            # Определяем правильный путь к SQL файлу
            current_dir = os.path.dirname(os.path.abspath(__file__))  # src/database/
            project_root = os.path.dirname(os.path.dirname(current_dir))  # корень проекта
            sql_file_path = os.path.join(project_root, 'data', 'sql', 'create_tables.sql')
            
            print(f" Поиск SQL файла по пути: {sql_file_path}")
            
            # Проверяем существование файла
            if not os.path.exists(sql_file_path):
                print(f"[X] Файл не найден: {sql_file_path}")
                print(" Текущая рабочая директория:", os.getcwd())
                print(" Директория скрипта:", current_dir)
                return False
            
            print(f"[V] Файл найден: {sql_file_path}")
            
            # Читаем SQL скрипт
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                
            print(f" SQL скрипт прочитан, размер: {len(sql_script)} символов")
            
            # Проверяем соединение с базой
            if not self.connection:
                print("[X] Нет соединения с базой данных")
                return False
                
            cursor = self.connection.cursor()
            
            # Выполняем SQL скрипт
            print(" Выполняем SQL скрипт...")
            cursor.executescript(sql_script)
            
            # Коммитим изменения
            self.connection.commit()
            
            # Проверяем, что таблицы действительно созданы
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"[V] Созданные таблицы: {tables}")
            
            if 'vacancies' in tables:
                print("[V] Таблица 'vacancies' успешно создана!")
                return True
            else:
                print("[X] Таблица 'vacancies' не найдена после создания!")
                return False
            
        except FileNotFoundError as e:
            print(f"[X] Файл SQL не найден: {e}")
            return False
        except PermissionError as e:
            print(f"[X] Нет прав доступа к файлу: {e}")
            return False
        except UnicodeDecodeError as e:
            print(f"[X] Ошибка кодировки файла: {e}")
            return False
        except sqlite3.Error as e:
            print(f"[X] Ошибка SQL: {e}")
            return False
        except Exception as e:
            print(f"[X] Неожиданная ошибка при создании таблиц: {e}")
            return False
            
    def tables_exist(self) -> bool:
        """
        Проверяет, существуют ли необходимые таблицы в базе данных.
        
        Возвращает:
            bool: True если все таблицы существуют
        """
        if not self.connection:
            return False
            
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            required_tables = ['vacancies', 'skills']
            missing_tables = [table for table in required_tables if table not in tables]
            
            if missing_tables:
                print(f"[!] Отсутствуют таблицы: {missing_tables}")
                return False
            else:
                print(f"[V] Все необходимые таблицы существуют: {tables}")
                return True
                
        except sqlite3.Error as e:
            print(f"[X] Ошибка при проверке таблиц: {e}")
            return False

    def vacancy_exists(self, vacancy_id: int) -> bool:
        """
        Проверяет, существует ли вакансия с заданным ID в базе.
        
        Аргументы:
            vacancy_id (int): ID вакансии из HH.ru
            
        Возвращает:
            bool: True если вакансия уже есть в базе
        """
        # Проверяем, что таблица существует
        if not self.tables_exist():
            print("[X] Таблица 'vacancies' не существует!")
            return False
            
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT id FROM vacancies WHERE id = ?", (vacancy_id,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            print(f"[X] Ошибка при проверке существования вакансии: {e}")
            return False
        
    def insert_vacancy(self, vacancy: Dict) -> bool:
        """
        Вставляет одну вакансию в базу данных.
        
        Аргументы:
            vacancy (Dict): Данные вакансии из API HH.ru
            
        Возвращает:
            bool: True если вакансия успешно сохранена
        """
        # Проверяем, что таблицы существуют
        if not self.tables_exist():
            print("[X] Таблицы не существуют! Создайте их сначала.")
            return False
        
        # Проверяем, что vacancy не None и является словарем
        if not vacancy or not isinstance(vacancy, dict):
            print(f"[X] Получена пустая или некорректная вакансия: {vacancy}")
            return False
        
        # Проверяем наличие обязательного ID
        vacancy_id = vacancy.get('id')
        if not vacancy_id:
            print(f"[X] У вакансии отсутствует ID: {vacancy.get('name', 'Без названия')}")
            return False
        
        # Проверяем, что вакансия уже не существует
        if self.vacancy_exists(vacancy_id):
            print(f"[!] Вакансия {vacancy_id} уже существует в базе")
            return False  
            
        try:
            cursor = self.connection.cursor()
            
            # Безопасное извлечение данных с проверкой на None
            salary = vacancy.get('salary') or {}
            area = vacancy.get('area') or {}
            employer = vacancy.get('employer') or {}
            snippet = vacancy.get('snippet') or {}
            experience = vacancy.get('experience') or {}
            schedule = vacancy.get('schedule') or {}
            employment = vacancy.get('employment') or {}
            
            # Извлекаем значения с защитой от None
            name = vacancy.get('name', '')
            area_name = area.get('name') if area else None
            salary_from = salary.get('from') if salary else None
            salary_to = salary.get('to') if salary else None
            salary_currency = salary.get('currency') if salary else None
            experience_name = experience.get('name') if experience else None
            schedule_name = schedule.get('name') if schedule else None
            employment_name = employment.get('name') if employment else None
            published_at = vacancy.get('published_at')
            employer_name = employer.get('name') if employer else None
            snippet_requirement = snippet.get('requirement')
            snippet_responsibility = snippet.get('responsibility')
            
            # Проверяем обязательные поля
            if not name:
                print(f"[!] У вакансии {vacancy_id} отсутствует название, пропускаем")
                return False
            
            # SQL запрос для вставки вакансии
            sql = """
            INSERT INTO vacancies 
            (id, name, area, salary_from, salary_to, salary_currency, 
             experience, schedule, employment, published_at, employer_name,
             snippet_requirement, snippet_responsibility)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Выполняем запрос с данными
            cursor.execute(sql, (
                vacancy_id,
                name,
                area_name,
                salary_from,
                salary_to,
                salary_currency,
                experience_name,
                schedule_name,
                employment_name,
                published_at,
                employer_name,
                snippet_requirement,
                snippet_responsibility
            ))
            
            # Сохраняем навыки если они есть
            skills = vacancy.get('key_skills', [])
            if skills and isinstance(skills, list):
                for skill in skills:
                    if skill and isinstance(skill, dict) and skill.get('name'):
                        self.insert_skill(vacancy_id, skill.get('name'))
            
            print(f"[V] Вакансия сохранена: {name[:50]}... (ID: {vacancy_id})")
            return True
            
        except sqlite3.IntegrityError as e:
            print(f"[X] Ошибка целостности данных для вакансии {vacancy_id}: {e}")
            return False
        except sqlite3.Error as e:
            print(f"[X] Ошибка SQL при вставке вакансии {vacancy_id}: {e}")
            return False
        except Exception as e:
            print(f"[X] Неожиданная ошибка при вставке вакансии {vacancy_id}: {e}")
            # Выводим отладочную информацию
            print(f"   Данные вакансии: {list(vacancy.keys()) if vacancy else 'NO DATA'}")
            return False
            
    def insert_skill(self, vacancy_id: int, skill_name: str):
        """
        Вставляет один навык для вакансии.
        
        Аргументы:
            vacancy_id (int): ID вакансии
            skill_name (str): Название навыка
        """
        if not vacancy_id or not skill_name:
            return
            
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "INSERT INTO skills (vacancy_id, skill_name) VALUES (?, ?)",
                (vacancy_id, skill_name)
            )
        except sqlite3.IntegrityError:
            # Игнорируем ошибки целостности (дубликаты)
            pass
        except sqlite3.Error as e:
            print(f"[x] Ошибка при вставке навыка '{skill_name}' для вакансии {vacancy_id}: {e}")
            
    def get_statistics(self) -> Dict:
        """
        Возвращает статистику по данным в базе.
        
        Возвращает:
            Dict: Статистика по вакансиям и навыкам
        """
        cursor = self.connection.cursor()
        stats = {}
        
        # Общее количество вакансий
        cursor.execute("SELECT COUNT(*) FROM vacancies")
        stats['total_vacancies'] = cursor.fetchone()[0]
        
        # Количество вакансий с указанной зарплатой
        cursor.execute("""
            SELECT COUNT(*) FROM vacancies 
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL
        """)
        stats['vacancies_with_salary'] = cursor.fetchone()[0]
        
        # Количество уникальных навыков
        cursor.execute("SELECT COUNT(DISTINCT skill_name) FROM skills")
        stats['unique_skills'] = cursor.fetchone()[0]
        
        # Топ-5 регионов по количеству вакансий
        cursor.execute("""
            SELECT area, COUNT(*) as count 
            FROM vacancies 
            WHERE area IS NOT NULL 
            GROUP BY area 
            ORDER BY count DESC 
            LIMIT 5
        """)
        stats['top_areas'] = dict(cursor.fetchall())
        
        return stats
        
    def close_connection(self):
        """Закрывает соединение с базой данных."""
        if self.connection:
            self.connection.close()
            print(" Соединение с базой данных закрыто")


# Пример использования
if __name__ == "__main__":
    print(" Запуск тестирования DatabaseManager...")
    
    # Создаем менеджер базы данных
    db_manager = DatabaseManager()
    
    # Подключаемся к базе
    print("\n Подключение к базе данных...")
    if not db_manager.create_connection():
        print("[x] Не удалось подключиться к базе данных")
        exit(1)
    
    # Проверяем существование таблиц
    print("\n Проверка существования таблиц...")
    if not db_manager.tables_exist():
        print("\n Создание таблиц...")
        if not db_manager.create_tables():
            print("[X] Не удалось создать таблицы")
            db_manager.close_connection()
            exit(1)
    else:
        print(" Таблицы уже существуют")
    
    # Тестовая вакансия для проверки
    print("\n Тестирование вставки вакансии...")
    test_vacancy = {
        'id': 999999,  
        'name': 'Тестовая вакансия - Инженер',
        'area': {'name': 'Москва'},
        'salary': {'from': 50000, 'to': 80000, 'currency': 'RUR'},
        'experience': {'name': 'от 1 года до 3 лет'},
        'schedule': {'name': 'полный день'},
        'employment': {'name': 'полная занятость'},
        'published_at': '2023-10-01T10:00:00+0300',
        'employer': {'name': 'Тестовая компания'},
        'snippet': {
            'requirement': 'Высшее образование, опыт работы',
            'responsibility': 'Разработка технологических процессов'
        },
        'key_skills': [
            {'name': 'Python'},
            {'name': 'SQL'}, 
            {'name': 'AutoCAD'}
        ]
    }
    
    # Пытаемся вставить вакансию
    success = db_manager.insert_vacancy(test_vacancy)
    if success:
        print("Тестовая вакансия успешно сохранена")
    else:
        print("[X] Не удалось сохранить тестовую вакансию")
    
    # Получаем статистику
    print("\n Статистика базы данных:")
    try:
        stats = db_manager.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    except Exception as e:
        print(f"[X] Ошибка при получении статистики: {e}")
    
    # Закрываем соединение
    print("\n Закрытие соединения...")
    db_manager.close_connection()
    print("Тестирование завершено!")
