import requests
import json

def test_hh_api():
    """Простой тест API HH.ru"""
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "промышленность",
        "area": 113,  # Россия
        "per_page": 10,
        "page": 0
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Проверяем ошибки HTTP
        
        data = response.json()
        
        print(" API работает корректно!")
        print(f" Найдено вакансий: {data['found']}")
        print(f" Страниц: {data['pages']}")
        
        # Сохраняем пример данных
        with open("data/raw/sample_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Показываем первую вакансию
        if data["items"]:
            first_vacancy = data["items"][0]
            print("\n Пример вакансии:")
            print(f"Название: {first_vacancy.get('name')}")
            print(f"Зарплата: {first_vacancy.get('salary')}")
            print(f"Регион: {first_vacancy.get('area', {}).get('name')}")
            print(f"Опыт: {first_vacancy.get('experience', {}).get('name')}")
            
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"[X] Ошибка при запросе: {e}")
        return False

if __name__ == "__main__":
    test_hh_api()