import json
from collections import Counter
import os

def analyze_vacancies():
    # Путь к файлу
    file_path = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
    
    # Проверяем существование файла
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не найден!")
        return
    
    try:
        # Читаем JSON файл
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        print(f"Всего вакансий в файле: {len(data)}")
        print("-" * 50)
        
        # Собираем статистику по названиям вакансий
        vacancy_names = [vacancy.get('name', 'Не указано') for vacancy in data]
        vacancy_counter = Counter(vacancy_names)
        
        # Сортируем по убыванию количества
        sorted_vacancies = vacancy_counter.most_common()
        
        # Выводим отчет
        print("ОТЧЕТ ПО ВАКАНСИЯМ:")
        print("=" * 50)
        
        for vacancy_name, count in sorted_vacancies:
            print(f"{vacancy_name} - {count} вакансий")
            
    except json.JSONDecodeError as e:
        print(f"Ошибка чтения JSON файла: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

# Альтернативная версия с дополнительной статистикой
def analyze_vacancies_detailed():
    file_path = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
    
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не найден!")
        return
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        print(f"Всего вакансий в файле: {len(data)}")
        print("=" * 60)
        
        # Статистика по названиям вакансий
        vacancy_names = [vacancy.get('name', 'Не указано') for vacancy in data]
        vacancy_counter = Counter(vacancy_names)
        
        # Дополнительная статистика
        professional_roles = []
        areas = []
        
        for vacancy in data:
            # Собираем профессиональные роли
            if 'professional_roles' in vacancy and vacancy['professional_roles']:
                for role in vacancy['professional_roles']:
                    professional_roles.append(role.get('name', 'Не указано'))
            
            # Собираем города
            if 'area' in vacancy and vacancy['area']:
                areas.append(vacancy['area'].get('name', 'Не указано'))
        
        print("ТОП-20 ВАКАНСИЙ ПО НАЗВАНИЯМ:")
        print("-" * 60)
        for vacancy_name, count in vacancy_counter.most_common(20):
            print(f"{vacancy_name} - {count} вакансий")
        
        print("\n" + "=" * 60)
        print("ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА:")
        print("-" * 60)
        
        # Статистика по профессиональным ролям
        if professional_roles:
            role_counter = Counter(professional_roles)
            print("\nТОП-10 ПРОФЕССИОНАЛЬНЫХ РОЛЕЙ:")
            for role, count in role_counter.most_common(10):
                print(f"{role} - {count}")
        
        # Статистика по городам
        if areas:
            area_counter = Counter(areas)
            print("\nТОП-10 ГОРОДОВ:")
            for area, count in area_counter.most_common(10):
                print(f"{area} - {count} вакансий")
                
    except Exception as e:
        print(f"Произошла ошибка: {e}")

# Запуск основной функции
if __name__ == "__main__":
    analyze_vacancies()
    
    # Раскомментируйте следующую строку для расширенной версии
    # analyze_vacancies_detailed()