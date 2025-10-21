import requests
import time
import json
import os
import concurrent.futures
from threading import Lock
from typing import Dict, List, Optional, Set

class IndustrialFilter:
    """
    Фильтр для массового сбора промышленных вакансий.
    """
    
    def __init__(self):
        self.base_url = "https://api.hh.ru/vacancies"
        self.delay = 1.0  # Увеличили задержку для избежания блокировки API
        self.lock = Lock()
        self.total_collected = 0
        
        # Супер-расширенный список промышленных профессий
        self.industrial_keywords = [
            # МАШИНОСТРОЕНИЕ И МЕТАЛЛООБРАБОТКА
            "инженер-механик", "инженер-технолог", "инженер-конструктор", 
            "инженер-проектировщик", "инженер-метролог", "инженер-электроник",
            "инженер-гальваник", "инженер-литейщик", "инженер-сварщик",
            "инженер-теплотехник", "инженер-холодильщик", "инженер-вентиляционщик",
            
            # РАБОЧИЕ ПРОФЕССИИ - МЕТАЛЛООБРАБОТКА
            "сварщик", "токарь", "фрезеровщик", "шлифовщик", "расточник",
            "слесарь", "слесарь-сборщик", "слесарь-ремонтник", "слесарь-инструментальщик",
            "станочник", "оператор станков", "наладчик", "наладчик станков",
            "оператор ЧПУ", "программист ЧПУ", "оператор станков с ЧПУ",
            "резчик", "строгальщик", "долбежник", "зуборезчик", "фрезеровщик-универсал",
            "карусельщик", "револьверщик", "радиально-сверловщик",
            
            # КИПиА И АВТОМАТИЗАЦИЯ
            "инженер КИПиА", "киповец", "инженер АСУ ТП", "инженер-автоматизатор",
            "инженер по автоматизации", "слесарь КИПиА", "электромеханик КИПиА",
            "наладчик КИПиА", "инженер по АСУ ТП", "специалист КИПиА", 
            "мастер КИПиА", "инженер по КИП", "инженер по приборам",
            "инженер по автоматике", "инженер по средствам автоматизации",
            "инженер-метролог", "метролог", "инженер по контрольно-измерительным приборам",
            "инженер по наладке", "инженер по испытаниям",
            
            # ЭЛЕКТРОТЕХНИКА И ЭНЕРГЕТИКА
            "инженер-энергетик", "электромонтер", "электромонтажник", 
            "машинист турбины", "машинист котельной", "монтер", "электрослесарь",
            "инженер-теплотехник", "инженер-электрик", "электротехник",
            "электромеханик", "электрогазосварщик", "электросварщик",
            "кабельщик", "электромонтер по ремонту", "электромонтер по обслуживанию",
            "инженер по релейной защите", "инженер по эксплуатации",
            
            # МЕТАЛЛУРГИЯ
            "металлург", "сталевар", "разливщик", "прокатчик", "волочильщик",
            "термист", "ковшевой", "печевой", "плавильщик", "литейщик",
            "вальцовщик", "горновой", "доменщик", "конвертерщик", "нагревальщик",
            "прессовщик", "прокатчик-раскатчик", "разметчик",
            
            # ХИМИЧЕСКАЯ ПРОМЫШЛЕННОСТЬ
            "аппаратчик", "оператор технологических установок", "химик-технолог",
            "лаборант химического анализа", "оператор химводоочистки",
            "аппаратчик приготовления химических растворов", "аппаратчик синтеза",
            "аппаратчик дистилляции", "аппаратчик экстрагирования",
            "машинист компрессорных установок", "машинист насосных установок",
            
            # НЕФТЕГАЗОВАЯ ПРОМЫШЛЕННОСТЬ
            "оператор ДНГ", "машинист технологических насосов", "оператор товарный",
            "оператор по добыче нефти", "оператор по подготовке скважин",
            "оператор по исследованию скважин", "оператор по ремонту скважин",
            "машинист буровой установки", "помощник бурильщика", "бурильщик",
            "оператор технологических установок нефтепереработки",
            
            # ПИЩЕВАЯ ПРОМЫШЛЕННОСТЬ
            "инженер-технолог пищевого производства", "аппаратчик пищевого производства",
            "оператор линии", "пекарь-мастер", "технолог молочного производства",
            "технолог мясного производства", "технолог хлебобулочного производства",
            "аппаратчик пастеризации", "аппаратчик розлива", "аппаратчик сушки",
            "машинист фасовочных машин", "оператор автоматических линий",
            
            # СТРОИТЕЛЬСТВО И МОНТАЖ
            "инженер-строитель", "монтажник", "монтажник оборудования", 
            "монтажник технологических трубопроводов", "монтажник стальных конструкций",
            "монтажник железобетонных конструкций", "такелажник", "крановщик",
            "машинист крана", "машинист экскаватора", "машинист бульдозера",
            
            # ТРАНСПОРТ И ЛОГИСТИКА
            "механик автомобильный", "электрик автомобильный", "слесарь по ремонту автомобилей",
            "моторист", "аккумуляторщик", "вулканизаторщик", "жестянщик",
            "маляр", "слесарь-арматурщик",
            
            # ОБЩИЕ ПРОМЫШЛЕННЫЕ ПРОФЕССИИ
            "механик", "электрик", "мастер", "начальник участка", "диспетчер",
            "контролер ОТК", "техник-технолог", "нормировщик", "техник-конструктор",
            "лаборант", "испытатель", "дефектовщик", "комплектовщик",
            "кладовщик", "упаковщик", "фасовщик"
        ]
        
        # Расширенный список исключений
        self.exclude_keywords = [
            # IT-профессии
            "программист", "разработчик", "developer", "frontend", "backend",
            "fullstack", "software", "приложений", "веб", "web", "ios", "android",
            "qa", "тестировщик", "тестер", "devops", "data science", "data scientist",
            "аналитик данных", "machine learning", "ai", "api", "sql", "python",
            "java", "javascript", "c++", "c#", "php", "ruby", "go", "kotlin",
            "react", "angular", "vue", "node.js", "django", "flask", "системный администратор",
            "администратор баз данных", "бэкенд", "фронтенд",
            
            # Офисные и бизнес-профессии
            "менеджер", "маркетолог", "дизайнер", "копирайтер", "контент-менеджер",
            "сео", "smm", "hr", "рекрутер", "бухгалтер", "экономист", "финансист",
            "юрист", "адвокат", "администратор", "офис-менеджер", "секретарь",
            "помощник", "ассистент", "торговый представитель", "продавец",
            "консультант", "оператор call-центра", "логист", "закупщик",
            "аналитик", "архитектор", "бизнес-аналитик",
            
            # Другие не-промышленные сферы
            "врач", "медицинский", "учитель", "преподаватель", "педагог",
            "психолог", "социальный работник", "повар", "официант", "бармен",
            "водитель", "курьер", "грузчик", "охранник", "клининг", "уборщик",
            "парикмахер", "косметолог", "визажист"
        ]

    def get_industrial_terms_by_industry(self, industry: str = "all") -> List[str]:
        """Возвращает термины для конкретной промышленной отрасли"""
        
        base_terms = [
            # Общие производственные термины
            'станок', 'оборудование', 'производство', 'цех', 'завод', 'фабрика',
            'механизм', 'деталь', 'чертеж', 'технология', 'сварка', 'токарная',
            'фрезерная', 'литье', 'металл', 'обработка', 'сборка', 'монтаж',
            'ремонт', 'техобслуживание', 'наладка', 'регулировка', 'испытание',
            'контроль качества', 'отк', 'техпроцесс', 'операция', 'переход',
        ]
        
        industry_specific_terms = {
            "automation": [
                'кипиа', 'асу тп', 'контрольно-измерительные приборы', 'автоматизация',
                'плуа', 'scada', 'телемеханика', 'сенсор', 'датчик', 'реле', 'контроллер',
                'исполнительный механизм', 'задвижка', 'клапан', 'регулятор', 'преобразователь',
                'измерительный прибор', 'манометр', 'термопара', 'расходомер', 'уровнемер',
                'автоматизированная система', 'plc', 'программируемый контроллер', 'hmi',
                'операторский интерфейс', 'шина profibus', 'modbus', 'fieldbus',
                'диспетчеризация', 'телеметрия', 'противоаварийная защита'
            ],
            "metallurgy": [
                'сталь', 'алюминий', 'медь', 'прокат', 'плавка', 'ковш', 'печь',
                'мартен', 'конвертер', 'разливка', 'прокатный стан', 'металлургический',
                'литье', 'ковка', 'штамповка', 'прокатка', 'волочение', 'прессование'
            ],
            "chemical": [
                'реактор', 'синтез', 'химический', 'реакция', 'катализатор',
                'дистилляция', 'ректификация', 'полимер', 'пластмасса', 'химводоочистка',
                'нейтрализация', 'осаждение', 'кристаллизация', 'фильтрация'
            ],
            "oil_gas": [
                'нефть', 'газ', 'скважина', 'месторождение', 'переработка',
                'нефтехимия', 'буровая', 'сепаратор', 'ректификация', 'дегидратация',
                'стабилизация', 'фракционирование', 'каталитический крекинг'
            ],
            "energy": [
                'энергия', 'турбина', 'генератор', 'трансформатор', 'подстанция',
                'электроснабжение', 'теплоснабжение', 'котельная', 'энергоблок',
                'релейная защита', 'автоматика', 'диспетчерский щит'
            ],
            "food": [
                'линия розлива', 'пастеризация', 'стерилизация', 'консервирование',
                'переработка', 'пищевое производство', 'упаковка', 'фасовка',
                'гомогенизация', 'пастеризатор', 'автоклав', 'сушильная установка'
            ],
            "automotive": [
                'автомобиль', 'автосборочный', 'кузов', 'двигатель', 'трансмиссия',
                'конвейер', 'автозавод', 'сборочная линия', 'окрасочная камера'
            ]
        }
        
        if industry == "all":
            all_terms = base_terms
            for terms in industry_specific_terms.values():
                all_terms.extend(terms)
            return list(set(all_terms))
        else:
            return base_terms + industry_specific_terms.get(industry, [])

    def is_industrial_vacancy(self, vacancy: Dict) -> bool:
        name = vacancy.get('name', '').lower()
        
        # Проверка исключений
        for exclude_word in self.exclude_keywords:
            if exclude_word in name:
                return False
        
        # Проверка промышленных профессий
        for industrial_word in self.industrial_keywords:
            if industrial_word in name:
                return True
        
        # Анализ описания
        snippet = vacancy.get('snippet', {})
        requirement = snippet.get('requirement', '').lower() if snippet.get('requirement') else ''
        responsibility = snippet.get('responsibility', '').lower() if snippet.get('responsibility') else ''
        
        description = requirement + ' ' + responsibility
        
        # Используем расширенный список терминов
        industrial_terms = self.get_industrial_terms_by_industry("all")
        
        # Особый случай для КИПиА - снижаем порог для терминов автоматизации
        automation_terms = self.get_industrial_terms_by_industry("automation")
        automation_count = sum(1 for term in automation_terms if term in description)
        
        if automation_count >= 2:
            return True
        
        industrial_terms_count = sum(1 for term in industrial_terms if term in description)
        
        return industrial_terms_count >= 2

    def search_by_industry_region(self, profession: str, industry_codes: List[int], 
                                region_code: int, max_pages: int = 15) -> List[Dict]:
        """Поиск вакансий по региону с увеличенным количеством страниц"""
        industrial_vacancies = []
        
        for industry in industry_codes:
            for page in range(max_pages):
                try:
                    params = {
                        "text": profession,
                        "area": region_code,
                        "per_page": 100,
                        "page": page,
                        "industry": industry,
                        "specialization": 7,
                        "only_with_salary": False
                    }
                    
                    response = requests.get(self.base_url, params=params, timeout=15)
                    data = response.json()
                    
                    vacancies = data.get('items', [])
                    
                    filtered = []
                    for v in vacancies:
                        try:
                            if self.is_industrial_vacancy(v):
                                filtered.append(v)
                        except Exception:
                            continue
                    
                    industrial_vacancies.extend(filtered)
                    
                    # Останавливаемся если страниц больше нет
                    if page >= data.get('pages', 0) - 1 or not vacancies:
                        break
                        
                    time.sleep(self.delay)  # Используем увеличенную задержку
                    
                except Exception as e:
                    print(f"Ошибка запроса {profession} стр {page}: {e}")
                    break
        
        return industrial_vacancies

    def get_industry_codes(self, industry: str) -> List[int]:
        """Возвращает коды отраслей для конкретной промышленности"""
        industry_mapping = {
            "машиностроение": [7, 25],
            "кипиа_автоматизация": [7, 25, 26, 29, 30, 31, 28],
            "энергетика": [31],
            "металлургия": [26],
            "химическая": [29],
            "нефтегазовая": [30],
            "пищевая": [32],
            "строительство": [28],
            "транспорт": [27],
            "общие": [7, 25, 26, 27, 28, 29, 30, 31, 32]
        }
        return industry_mapping.get(industry, [7])

    def collect_massive_industrial_vacancies(self, target_count: int = 50000) -> Dict:
        """Массовый сбор вакансий с многопоточностью"""
        print(f"=== МАССОВЫЙ СБОР {target_count} ПРОМЫШЛЕННЫХ ВАКАНСИЙ ===")
        
        # Основные промышленные профессии для массового поиска
        core_professions = [
            "инженер", "техник", "слесарь", "электрик", "механик",
            "сварщик", "токарь", "фрезеровщик", "оператор", "наладчик",
            "машинист", "аппаратчик", "монтажник", "контролер"
        ]
        
        # Крупные регионы России
        regions = {
            "Москва": 1,
            "Санкт-Петербург": 2,
            "Новосибирск": 4,
            "Екатеринбург": 3,
            "Нижний Новгород": 66,
            "Казань": 88,
            "Челябинск": 104,
            "Омск": 68,
            "Самара": 51,
            "Ростов-на-Дону": 39,
            "Уфа": 99,
            "Красноярск": 54,
            "Пермь": 72,
            "Воронеж": 26,
            "Волгоград": 24,
            "Краснодар": 53,
            "Саратов": 64,
            "Тюмень": 5,
            "Ижевск": 6,
            "Барнаул": 63,
            "Иркутск": 76,
            "Ульяновск": 73,
            "Владивосток": 75,
            "Ярославль": 74,
            "Кемерово": 42,
            "Тверь": 69,
            "Ставрополь": 26,
            "Сочи": 237
        }
        
        all_vacancies = {}
        search_combinations = []
        
        # Создаем комбинации для поиска
        for profession in core_professions:
            for region_name, region_code in regions.items():
                search_combinations.append((profession, region_name, region_code))
        
        print(f"Создано комбинаций для поиска: {len(search_combinations)}")
        
        # Многопоточный сбор
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            future_to_combo = {
                executor.submit(self.mass_search, prof, reg_code, 20): (prof, reg_name) 
                for prof, reg_name, reg_code in search_combinations
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_combo):
                prof, reg_name = future_to_combo[future]
                completed += 1
                
                try:
                    vacancies = future.result()
                    if vacancies:
                        key = f"{prof}_{reg_name}"
                        all_vacancies[key] = vacancies
                        
                        with self.lock:
                            self.total_collected += len(vacancies)
                            
                        if completed % 10 == 0:
                            print(f"Прогресс: {completed}/{len(search_combinations)} | Собрано: {self.total_collected}/{target_count}")
                            
                        if self.total_collected >= target_count:
                            print(f"Достигнута целевая отметка: {self.total_collected} вакансий")
                            executor.shutdown(wait=False)
                            break
                            
                except Exception as e:
                    print(f"Ошибка в потоке {prof}_{reg_name}: {e}")
        
        return all_vacancies
    
    def mass_search(self, profession: str, region: int, max_pages: int = 20) -> List[Dict]:
        """Упрощенный поиск для массового сбора с увеличенными страницами"""
        vacancies = []
        
        for page in range(max_pages):
            try:
                params = {
                    "text": profession,
                    "area": region,
                    "per_page": 100,
                    "page": page,
                    "specialization": 7,
                    "only_with_salary": False
                }
                
                response = requests.get(self.base_url, params=params, timeout=15)
                data = response.json()
                
                page_vacancies = data.get('items', [])
                
                # Упрощенная фильтрация для скорости
                filtered = [v for v in page_vacancies if self.quick_industrial_filter(v)]
                vacancies.extend(filtered)
                
                # Проверяем наличие следующих страниц
                if not page_vacancies or page >= data.get('pages', 0) - 1:
                    break
                    
                time.sleep(0.3)  # Увеличенная задержка между страницами
                
            except Exception as e:
                print(f"Ошибка массового поиска {profession} стр {page}: {e}")
                break
        
        return vacancies
    
    def quick_industrial_filter(self, vacancy: Dict) -> bool:
        """Быстрая фильтрация для массового сбора"""
        name = vacancy.get('name', '').lower()
        
        # Быстрая проверка исключений
        quick_exclude = ["программист", "разработчик", "менеджер", "бухгалтер", 
                        "маркетолог", "дизайнер", "юрист", "консультант"]
        if any(excl in name for excl in quick_exclude):
            return False
        
        # Быстрая проверка промышленных профессий
        quick_include = ["инженер", "техник", "слесарь", "электрик", "механик", 
                        "сварщик", "токарь", "фрезеровщик", "оператор", "наладчик",
                        "машинист", "аппаратчик", "монтажник", "контролер", "кипиа"]
        if any(inc in name for inc in quick_include):
            return True
        
        return False

    def analyze_industrial_data(self, all_vacancies: Dict):
        """Анализ собранных данных"""
        print("=" * 60)
        print("АНАЛИЗ СОБРАННЫХ ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
        print("=" * 60)
        
        total_vacancies = sum(len(v) for v in all_vacancies.values())
        
        if total_vacancies == 0:
            print("Не найдено промышленных вакансий")
            return
        
        # Статистика по профессиям
        profession_stats = {}
        for key, vacancies in all_vacancies.items():
            profession = key.split('_')[0]
            if profession not in profession_stats:
                profession_stats[profession] = 0
            profession_stats[profession] += len(vacancies)
        
        print(f"Всего собрано вакансий: {total_vacancies}")
        print("\nРАСПРЕДЕЛЕНИЕ ПО ПРОФЕССИЯМ:")
        for profession, count in sorted(profession_stats.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_vacancies) * 100
            print(f"  {profession}: {count} ({percentage:.1f}%)")
        
        # Статистика по зарплатам
        salary_stats = self.analyze_salaries(all_vacancies)
        if salary_stats:
            print(f"\nСТАТИСТИКА ПО ЗАРПЛАТАМ:")
            print(f"  Вакансий с зарплатой: {salary_stats['with_salary']}/{total_vacancies} ({salary_stats['salary_percentage']:.1f}%)")
            if salary_stats['avg_salary'] > 0:
                print(f"  Средняя зарплата: {salary_stats['avg_salary']:.0f} руб.")
    
    def analyze_salaries(self, all_vacancies: Dict) -> Dict:
        """Анализ зарплат в собранных вакансиях"""
        total_with_salary = 0
        salary_sum = 0
        salary_count = 0
        
        for vacancies in all_vacancies.values():
            for vacancy in vacancies:
                salary = vacancy.get('salary')
                if salary and (salary.get('from') or salary.get('to')):
                    total_with_salary += 1
                    
                    salary_val = 0
                    if salary.get('from'):
                        salary_val = salary['from']
                    elif salary.get('to'):
                        salary_val = salary['to'] * 0.8
                    
                    if salary_val > 0:
                        salary_sum += salary_val
                        salary_count += 1
        
        total_vacancies = sum(len(v) for v in all_vacancies.values())
        salary_percentage = (total_with_salary / total_vacancies) * 100 if total_vacancies else 0
        avg_salary = salary_sum / salary_count if salary_count else 0
        
        return {
            'with_salary': total_with_salary,
            'salary_percentage': salary_percentage,
            'avg_salary': avg_salary
        }

    def save_industrial_data(self, all_vacancies: Dict, filename_suffix: str = ""):
        """Сохранение данных в файл"""
        timestamp = int(time.time())
        suffix = f"_{filename_suffix}" if filename_suffix else ""
        filename = f"data/raw/industrial_vacancies_{timestamp}{suffix}.json"
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        total_vacancies = sum(len(v) for v in all_vacancies.values())
        
        save_data = {
            "metadata": {
                "collection_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_vacancies": total_vacancies,
                "description": f"Массив промышленных вакансий ({total_vacancies} записей)"
            },
            "vacancies_by_query": all_vacancies
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2)
        
        print(f"Данные сохранены: {filename}")
        return filename

    def collect_real_industrial_vacancies(self, max_pages: int = 10) -> Dict:
            """
            Сбор промышленных вакансий - адаптированная версия для main.py
            
            Args:
                max_pages (int): Максимальное количество страниц для сбора
                
            Returns:
                Dict: Словарь с вакансиями, сгруппированными по запросам
            """
            print("🔍 Начинаем сбор промышленных вакансий...")
            
            # Основные промышленные запросы
            search_queries = [
                "инженер промышленность",
                "слесарь производство", 
                "технолог завод",
                "оператор оборудование",
                "наладчик станок",
                "механик промышленность",
                "электрик промышленность",
                "сварщик производство",
                "токарь металлообработка",
                "фрезеровщик механический"
            ]
            
            all_vacancies = {}
            
            for query in search_queries:
                print(f"📝 Поиск по запросу: '{query}'")
                
                try:
                    # Используем массовый поиск для каждого запроса
                    vacancies = self.mass_search(query, 113, max_pages)  # 113 - Россия
                    
                    if vacancies:
                        all_vacancies[query] = vacancies
                        print(f"✅ Найдено {len(vacancies)} вакансий по запросу '{query}'")
                    else:
                        print(f"⚠️ По запросу '{query}' вакансий не найдено")
                        
                    # Пауза между запросами
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"❌ Ошибка при поиске по запросу '{query}': {e}")
                    continue
            
            total_vacancies = sum(len(v) for v in all_vacancies.values())
            print(f"🎯 Всего собрано {total_vacancies} промышленных вакансий")
            
            return all_vacancies

if __name__ == "__main__":
    print("ЗАПУСК МАССОВОГО СБОРА ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
    print("=" * 60)
    
    filter_tool = IndustrialFilter()
    
    start_time = time.time()
    
    # Массовый сбор 50,000+ вакансий
    industrial_data = filter_tool.collect_massive_industrial_vacancies(50000)
    
    collection_time = time.time() - start_time
    
    # Анализ результатов
    filter_tool.analyze_industrial_data(industrial_data)
    
    # Сохранение данных
    filename = filter_tool.save_industrial_data(industrial_data, "massive")
    
    total_vacancies = sum(len(v) for v in industrial_data.values())
    
    print(f"\n" + "=" * 60)
    print("СБОР ЗАВЕРШЕН!")
    print("=" * 60)
    print(f"Общее время: {collection_time:.1f} сек ({collection_time/60:.1f} мин)")
    print(f"Собрано вакансий: {total_vacancies}")
    print(f"Скорость сбора: {total_vacancies/collection_time:.1f} вакансий/сек")
    print(f"Файл с данными: {filename}")