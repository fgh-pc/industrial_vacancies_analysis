# test_database.py
import asyncio
import os
from db_manager import DatabaseManager

async def test_database():
    """Тестирование работы с базой данных"""
    print(" Тестирование DatabaseManager...")
    
    # Тестовая база
    test_db = "test_industrial.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    db_manager = DatabaseManager(test_db)
    
    try:
        # Подключение
        if not await db_manager.create_connection():
            print("[X] Ошибка подключения к БД")
            return False
        
        # Создание таблиц
        if not await db_manager.create_tables_async():
            print("[X] Ошибка создания таблиц")
            return False
        
        # Проверка таблиц
        if not await db_manager.tables_exist():
            print("[X] Таблицы не созданы")
            return False
        
        # Тестовая вакансия
        test_vacancy = {
            'id': 999999,
            'name': 'Тестовый инженер-разработчик',
            'area': {'name': 'Москва'},
            'salary': {'from': 100000, 'to': 150000, 'currency': 'RUR'},
            'experience': {'name': 'от 3 лет'},
            'schedule': {'name': 'полный день'},
            'employment': {'name': 'полная занятость'},
            'published_at': '2024-01-15T10:00:00+0300',
            'employer': {'name': 'Тестовая компания'},
            'snippet': {
                'requirement': 'Опыт работы с Python, SQL',
                'responsibility': 'Разработка промышленного ПО'
            },
            'key_skills': [
                {'name': 'Python'},
                {'name': 'SQL'},
                {'name': 'Docker'}
            ]
        }
        
        # Вставка вакансии
        success = await db_manager.insert_vacancy(test_vacancy)
        if not success:
            print("[X] Ошибка вставки вакансии")
            return False
        
        # Проверка существования
        exists = await db_manager.vacancy_exists(999999)
        if not exists:
            print("[X] Вакансия не найдена в БД")
            return False
        
        # Массовая вставка
        vacancies_batch = []
        for i in range(100, 110):
            vacancy = test_vacancy.copy()
            vacancy['id'] = i
            vacancy['name'] = f'Тестовая вакансия {i}'
            vacancies_batch.append(vacancy)
        
        inserted_count = await db_manager.insert_vacancy_batch(vacancies_batch)
        if inserted_count < 5:
            print("[X] Массовая вставка не работает")
            return False
        
        # Статистика
        stats = await db_manager.get_statistics()
        print(" Статистика БД:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        print("[V] DatabaseManager работает корректно!")
        return True
        
    except Exception as e:
        print(f"[X] Ошибка тестирования БД: {e}")
        return False
    finally:
        await db_manager.close_connection()
        # Очистка
        if os.path.exists(test_db):
            os.remove(test_db)

if __name__ == "__main__":
    asyncio.run(test_database())