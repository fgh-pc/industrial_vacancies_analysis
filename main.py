import time
from src.api.hh_api_client import IndustrialFilter  
from src.database.db_manager import DatabaseManager

def main():
    """
    Главный скрипт для сбора вакансий и сохранения в базу данных.
    """
    print("НАЧАЛО СБОРА ДАННЫХ О ВАКАНСИЯХ")
    
    # Инициализируем фильтр промышленных вакансий и менеджер базы данных
    industrial_filter = IndustrialFilter()  
    db_manager = DatabaseManager()
    
    try:
        # Подключаемся к базе данных и создаем таблицы
        print("\n1.  Подключение к базе данных...")
        if not db_manager.create_connection():
            print("[X] Не удалось подключиться к базе данных. Завершение работы.")
            return
        
        print("Подключение к базе данных установлено")
        
        # Создаем таблицы если их нет
        print("\n2. Создание таблиц в базе данных...")
        if not db_manager.create_tables():
            print("[X] Не удалось создать таблицы. Завершение работы.")
            return
        
        print("Таблицы успешно созданы")
        
        # Получаем промышленные вакансии с фильтром
        print("\n3. Получение промышленных вакансий с HH.ru...")
        industrial_data = industrial_filter.collect_real_industrial_vacancies()
        
        if not industrial_data:
            print("[X] Не удалось получить вакансии. Завершение работы.")
            return
        
        # Преобразуем данные в плоский список вакансий
        all_vacancies = []
        for profession, vacancies in industrial_data.items():
            all_vacancies.extend(vacancies)
        
        print(f"Получено {len(all_vacancies)} промышленных вакансий для обработки")
        
        # Шаг 3: Сохраняем сырые данные в JSON файл
        print("\n4. Сохранение сырых данных...")
        filename = industrial_filter.save_industrial_data(industrial_data)
        
        # Шаг 4: Сохраняем вакансии в базу данных
        print("\n5. Сохранение вакансий в базу данных...")
        successful_saves = 0
        duplicate_count = 0
        error_count = 0
        
        # Обрабатываем каждую вакансию
        for i, vacancy in enumerate(all_vacancies):
            # Показываем прогресс каждые 20 вакансий
            if i % 20 == 0:
                print(f"   Обработано {i}/{len(all_vacancies)} вакансий...")
            
            # Пытаемся сохранить вакансию в базу
            if db_manager.insert_vacancy(vacancy):
                successful_saves += 1
            else:
                # Если не удалось, проверяем почему
                if db_manager.vacancy_exists(vacancy.get('id')):
                    duplicate_count += 1
                else:
                    error_count += 1
        
        # Шаг 5: Получаем статистику и выводим результаты
        print("\n6. Формирование отчета...")
        stats = db_manager.get_statistics()
        
        print("\n=== РЕЗУЛЬТАТЫ СБОРА ДАННЫХ ===")
        print(f"Успешно сохранено: {successful_saves} промышленных вакансий")
        print(f"Пропущено дубликатов: {duplicate_count}")
        print(f"Ошибок при сохранении: {error_count}")
        print(f"Всего в базе: {stats['total_vacancies']} вакансий")
        print(f"Вакансий с зарплатой: {stats['vacancies_with_salary']}")
        print(f"Уникальных навыков: {stats['unique_skills']}")
        
        # Топ регионов
        print("\nТоп-5 регионов по вакансиям:")
        for region, count in stats['top_areas'].items():
            print(f"   {region}: {count} вакансий")
            
        # Сохраняем изменения в базе данных
        db_manager.connection.commit()
        
        print(f"\nСырые данные сохранены в: {filename}")
        print("Сбор промышленных вакансий завершен успешно!")
        
    except Exception as e:
        print(f"\n[X] Произошла непредвиденная ошибка: {e}")
        
    finally:
        # Всегда закрываем соединение с базой данных
        if db_manager.connection:
            db_manager.close_connection()
        
        print("\nРАБОТА ПРОГРАММЫ ЗАВЕРШЕНА ")

if __name__ == "__main__":
    main()