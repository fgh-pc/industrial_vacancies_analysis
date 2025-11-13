# test_api_client.py
import asyncio
import os
from hh_api_client import TurboHHAPIClient

async def test_api_client():
    """Тестирование API клиента"""
    print(" Тестирование TurboHHAPIClient...")
    
    client = TurboHHAPIClient()
    
    try:
        # Аутентификация
        await client.authenticate()
        print("[V] Аутентификация успешна")
        
        # Тестовый запрос
        test_data = await client.get_vacancies_page(
            search_text="инженер",
            area=1,  # Москва
            page=0,
            per_page=10
        )
        
        if not test_data or 'items' not in test_data:
            print("[X] Не удалось получить данные с API")
            return False
        
        vacancies = test_data.get('items', [])
        print(f"[V] Получено {len(vacancies)} тестовых вакансий")
        
        # Проверка структуры данных
        if vacancies:
            vacancy = vacancies[0]
            required_fields = ['id', 'name', 'area', 'employer']
            for field in required_fields:
                if field not in vacancy:
                    print(f"[X] Отсутствует поле {field} в ответе API")
                    return False
        
        print("[V] TurboHHAPIClient работает корректно!")
        return True
        
    except Exception as e:
        print(f"[X] Ошибка тестирования API: {e}")
        return False
    finally:
        await client.close()

async def test_small_collection():
    """Тест сбора небольшого количества данных"""
    print("\n Тестовый сбор данных...")
    
    client = TurboHHAPIClient()
    
    try:
        # Быстрый тестовый сбор
        results = await client.mass_turbo_collect(target_count=100)
        
        total_collected = sum(len(v) for v in results.values())
        print(f"[V] Собрано {total_collected} тестовых вакансий")
        
        if total_collected > 0:
            print("[V] Сбор данных работает корректно!")
            return True
        else:
            print("[X] Не собрано ни одной вакансии")
            return False
            
    except Exception as e:
        print(f"[X] Ошибка тестового сбора: {e}")
        return False
    finally:
        await client.close()

if __name__ == "__main__":
    # Тест аутентификации и базовых запросов
    auth_success = asyncio.run(test_api_client())
    
    # Тест сбора данных (опционально, может занять время)
    if auth_success:
        print("\nЗапустить тестовый сбор данных? (y/n)")
        if input().lower() == 'y':
            asyncio.run(test_small_collection())