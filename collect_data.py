from src.api.hh_api_client import HHAPIClient

def main():
    """Основной скрипт сбора данных"""
    client = HHAPIClient()
    
    # Собираем данные по разным запросам
    queries = [
        "промышленность",
        "инженер", 
        "технолог",
        "сварщик"
    ]
    
    for query in queries:
        print(f"\n=== Сбор данных по запросу: '{query}' ===")
        vacancies = client.get_all_vacancies(query, max_pages=5)
        filename = f"data/raw/vacancies_{query}.json"
        client.save_raw_data(vacancies, filename)

if __name__ == "__main__":
    main()