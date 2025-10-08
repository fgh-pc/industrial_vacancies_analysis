import requests
import time
import json
import os
from typing import Dict, List, Optional

class IndustrialFilter:
    """
    Фильтр для сбора промышленных вакансий.
    """
    
    def __init__(self):
        self.base_url = "https://api.hh.ru/vacancies"
        self.delay = 0.3
        
        self.industrial_keywords = [
            "инженер-механик", "инженер-технолог", "инженер-конструктор", 
            "инженер-энергетик", "сварщик", "токарь", "фрезеровщик", 
            "слесарь", "механик", "электрик", "наладчик", "оператор станков"
        ]
        
        self.exclude_keywords = [
            "программист", "разработчик", "developer", "frontend", "backend",
            "fullstack", "software", "приложений", "веб", "web"
        ]

    def is_industrial_vacancy(self, vacancy: Dict) -> bool:
        name = vacancy.get('name', '').lower()
        
        for exclude_word in self.exclude_keywords:
            if exclude_word in name:
                return False
        
        for industrial_word in self.industrial_keywords:
            if industrial_word in name:
                return True
        
        snippet = vacancy.get('snippet', {})
        requirement = snippet.get('requirement', '').lower() if snippet.get('requirement') else ''
        responsibility = snippet.get('responsibility', '').lower() if snippet.get('responsibility') else ''
        
        description = requirement + ' ' + responsibility
        
        industrial_terms = [
            'станок', 'оборудование', 'производство', 'цех', 'завод', 'фабрика',
            'механизм', 'деталь', 'чертеж', 'технология', 'сварка', 'токарная',
            'фрезерная', 'литье', 'металл'
        ]
        
        industrial_terms_count = sum(1 for term in industrial_terms if term in description)
        
        return industrial_terms_count >= 2

    def search_with_industrial_filter(self, profession: str, max_pages: int = 2) -> List[Dict]:
        industrial_vacancies = []
        
        for page in range(max_pages):
            params = {
                "text": f'"{profession}"',
                "area": 113,
                "per_page": 100,
                "page": page,
                "industry": 7,
                "specialization": 7
            }
            
            try:
                response = requests.get(self.base_url, params=params, timeout=10)
                data = response.json()
                
                vacancies = data.get('items', [])
                
                filtered = []
                for v in vacancies:
                    try:
                        if self.is_industrial_vacancy(v):
                            filtered.append(v)
                    except Exception as e:
                        print(f"Ошибка фильтрации вакансии: {e}")
                        continue
                
                industrial_vacancies.extend(filtered)
                
                print(f"'{profession}' страница {page+1}: {len(vacancies)} найдено, {len(filtered)} промышленных")
                
                if not vacancies or page >= data.get('pages', 0) - 1:
                    break
                    
                time.sleep(self.delay)
                
            except Exception as e:
                print(f"Ошибка запроса: {e}")
                break
        
        print(f"'{profession}': {len(industrial_vacancies)} промышленных")
        return industrial_vacancies

    def collect_real_industrial_vacancies(self) -> Dict:
        print("=== ПОИСК ПРОМЫШЛЕННЫХ ВАКАНСИЙ ===")
        
        search_queries = [
            "инженер-механик",
            "инженер-технолог", 
            "сварщик",
            "токарь",
            "фрезеровщик",
            "слесарь",
            "наладчик оборудования",
            "оператор станков"
        ]
        
        all_vacancies = {}
        
        for query in search_queries:
            print(f"Поиск: {query}")
            
            vacancies = self.search_with_industrial_filter(query, max_pages=2)
            
            if vacancies:
                all_vacancies[query] = vacancies
                print(f"Найдено промышленных: {len(vacancies)}")
                
                for i, vac in enumerate(vacancies[:2]):
                    salary = vac.get('salary')
                    salary_str = "не указана"
                    
                    if salary:
                        if salary.get('from'):
                            salary_str = f"от {salary['from']}"
                        if salary.get('to'):
                            salary_str += f" до {salary['to']}"
                        if salary.get('currency'):
                            salary_str += f" ({salary['currency']})"
                    
                    print(f"  {i+1}. {vac.get('name')}")
                    print(f"     Зарплата: {salary_str}")
                    print(f"     Регион: {vac.get('area', {}).get('name')}")
            else:
                print(f"Промышленных вакансий не найдено")
            
            time.sleep(0.5)
        
        return all_vacancies

    def analyze_industrial_data(self, all_vacancies: Dict):
        print("=" * 60)
        print("АНАЛИЗ ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
        print("=" * 60)
        
        total_vacancies = sum(len(v) for v in all_vacancies.values())
        
        if total_vacancies == 0:
            print("Не найдено промышленных вакансий")
            return
        
        with_salary = 0
        salary_sum = 0
        salary_count = 0
        
        for query, vacancies in all_vacancies.items():
            print(f"{query.upper()}: {len(vacancies)} вакансий")
            
            query_with_salary = 0
            query_salary_sum = 0
            query_salary_count = 0
            
            for i, vacancy in enumerate(vacancies[:2]):
                salary = vacancy.get('salary')
                
                if salary and (salary.get('from') or salary.get('to')):
                    query_with_salary += 1
                    
                    salary_val = 0
                    if salary.get('from'):
                        salary_val = salary['from']
                    elif salary.get('to'):
                        salary_val = salary['to'] * 0.8
                    
                    if salary_val > 0:
                        query_salary_sum += salary_val
                        query_salary_count += 1
                
                if i == 0:
                    print(f"   Пример: {vacancy.get('name')}")
                    print(f"   Регион: {vacancy.get('area', {}).get('name')}")
                    
                    salary_str = "не указана"
                    if salary:
                        if salary.get('from'):
                            salary_str = f"от {salary['from']}"
                        if salary.get('to'):
                            salary_str += f" до {salary['to']}"
                        if salary.get('currency'):
                            salary_str += f" ({salary['currency']})"
                    
                    print(f"   Зарплата: {salary_str}")
                    print(f"   Компания: {vacancy.get('employer', {}).get('name')}")
            
            salary_percentage = (query_with_salary / len(vacancies)) * 100 if vacancies else 0
            avg_salary = query_salary_sum / query_salary_count if query_salary_count else 0
            
            print(f"   Статистика:")
            print(f"      С зарплатой: {query_with_salary}/{len(vacancies)} ({salary_percentage:.1f}%)")
            if avg_salary > 0:
                print(f"      Средняя зарплата: {avg_salary:.0f} руб.")
            
            with_salary += query_with_salary
            salary_sum += query_salary_sum
            salary_count += query_salary_count
        
        print("=" * 60)
        print("ОБЩАЯ СТАТИСТИКА")
        print("=" * 60)
        print(f"Всего промышленных вакансий: {total_vacancies}")
        
        salary_percentage = (with_salary / total_vacancies) * 100 if total_vacancies else 0
        print(f"С указанной зарплатой: {with_salary} ({salary_percentage:.1f}%)")
        
        if salary_count > 0:
            avg_salary = salary_sum / salary_count
            print(f"Средняя зарплата: {avg_salary:.0f} руб.")
        
        print("РАСПРЕДЕЛЕНИЕ:")
        for query, vacancies in all_vacancies.items():
            percentage = (len(vacancies) / total_vacancies) * 100
            print(f"   {query}: {len(vacancies)} вакансий ({percentage:.1f}%)")

    def save_industrial_data(self, all_vacancies: Dict):
        timestamp = int(time.time())
        filename = f"data/raw/industrial_vacancies_{timestamp}.json"
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        save_data = {
            "metadata": {
                "collection_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_industrial_vacancies": sum(len(v) for v in all_vacancies.values()),
                "description": "Промышленные вакансии"
            },
            "vacancies_by_profession": all_vacancies
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2)
        
        print(f"Данные сохранены: {filename}")
        return filename


if __name__ == "__main__":
    print("ЗАПУСК ФИЛЬТРА ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
    print("=" * 60)
    
    filter_tool = IndustrialFilter()
    
    start_time = time.time()
    industrial_data = filter_tool.collect_real_industrial_vacancies()
    collection_time = time.time() - start_time
    
    filter_tool.analyze_industrial_data(industrial_data)
    
    filename = filter_tool.save_industrial_data(industrial_data)
    
    print(f"СБОР ЗАВЕРШЕН!")
    print(f"Время: {collection_time:.1f} сек")
    print(f"Файл: {filename}")
    
    total_vacancies = sum(len(v) for v in industrial_data.values())
    print(f"ИТОГИ:")
    print(f"   - Найдено промышленных вакансий: {total_vacancies}")
    print(f"   - Данные готовы для анализа")