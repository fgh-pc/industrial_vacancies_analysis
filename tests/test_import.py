try:
    from src.api.hh_api_client import HHAPIClient
    print("Модуль успешно импортирован!")
    
    client = HHAPIClient()
    print("Клиент создан успешно!")
    
    # Простой тест - получим 1 страницу вакансий
    vacancies = client.get_all_vacancies("промышленность", max_pages=1)
    print(f"Получено вакансий: {len(vacancies)}")
    
    # Сохраним результат
    client.save_raw_data(vacancies, "data/raw/test_import.json")
    print("Данные сохранены!")
    
except Exception as e:
    print(f"[X] Ошибка: {e}")
    import traceback
    traceback.print_exc()