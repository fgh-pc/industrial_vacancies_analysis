"""
Улучшенный клиент для работы с API HeadHunter.
Массовый сбор промышленных вакансий с интеллектуальной фильтрацией.
Интеграция для четвертого дедлайна - формирование полного датасета.
"""

import requests
import time
import json
import os
import concurrent.futures
from threading import Lock
from typing import Dict, List, Optional, Set, Any
import logging
from datetime import datetime, timedelta
import random


class HHAPIClient:
    """
    Улучшенный клиент для массового сбора промышленных вакансий с HH.ru.
    Оптимизирован для сбора 50,000+ вакансий промышленной отрасли.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        # Основные настройки API
        self.base_url = "https://api.hh.ru/vacancies"
        self.lock = Lock()
        self.total_collected = 0
        self.collected_ids: Set[str] = set()
        self.logger = self._setup_logger()
        
        # Конфигурация по умолчанию
        self.config = config or {
            'target_vacancies': 50000,
            'days_back': 730,
            'max_pages_per_query': 10,
            'requests_delay': 3.0,
            'max_workers': 4,
            'timeout': 30,
            'regions': self._get_default_regions(),
            'industrial_queries': self._get_default_queries()
        }
        
        # Инициализация фильтров промышленных вакансий
        self.industrial_keywords = self._load_industrial_keywords()
        self.exclude_keywords = self._load_exclude_keywords()
        self.industrial_terms = self._load_industrial_terms()

    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования."""
        logger = logging.getLogger('HHAPIClient')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
        return logger

    def _get_default_regions(self) -> Dict[str, int]:
        """Возвращает словарь промышленных регионов России."""
        return {
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
            "Томск": 96,
            "Кемерово": 42,
            "Россия": 113
        }

    def _get_default_queries(self) -> List[str]:
        """Возвращает список поисковых запросов для промышленности."""
        return [
            "инженер промышленность",
            "слесарь производство", 
            "технолог завод",
            "оператор оборудование",
            "наладчик станок",
            "механик промышленность",
            "электрик промышленность",
            "сварщик производство",
            "токарь металлообработка",
            "фрезеровщик механический",
            "кипиа автоматизация",
            "энергетик промышленность",
            "аппаратчик химический",
            "металлург производство",
            "машинист промышленность",
            "контролер отк",
            "мастер цех",
            "начальник участка производство"
        ]

    def _load_industrial_keywords(self) -> List[str]:
        """Загружает ключевые слова промышленных профессий."""
        return [
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
            
            # КИПиА И АВТОМАТИЗАЦИЯ
            "инженер КИПиА", "киповец", "инженер АСУ ТП", "инженер-автоматизатор",
            "инженер по автоматизации", "слесарь КИПиА", "электромеханик КИПиА",
            "наладчик КИПиА", "инженер по АСУ ТП", "специалист КИПиА", 
            "мастер КИПиА", "инженер по КИП", "инженер по приборам",
            "инженер по автоматике", "инженер по средствам автоматизации",
            
            # ЭЛЕКТРОТЕХНИКА И ЭНЕРГЕТИКА
            "инженер-энергетик", "электромонтер", "электромонтажник", 
            "машинист турбины", "машинист котельной", "монтер", "электрослесарь",
            "инженер-теплотехник", "инженер-электрик", "электротехник",
            "электромеханик", "электрогазосварщик", "электросварщик",
            
            # МЕТАЛЛУРГИЯ
            "металлург", "сталевар", "разливщик", "прокатчик", "волочильщик",
            "термист", "ковшевой", "печевой", "плавильщик", "литейщик",
            
            # ХИМИЧЕСКАЯ ПРОМЫШЛЕННОСТЬ
            "аппаратчик", "оператор технологических установок", "химик-технолог",
            "лаборант химического анализа", "оператор химводоочистки",
            
            # НЕФТЕГАЗОВАЯ ПРОМЫШЛЕННОСТЬ
            "оператор ДНГ", "машинист технологических насосов", "оператор товарный",
            "оператор по добыче нефти", "оператор по подготовке скважин",
            
            # ПИЩЕВАЯ ПРОМЫШЛЕННОСТЬ
            "инженер-технолог пищевого производства", "аппаратчик пищевого производства",
            "оператор линии", "пекарь-мастер", "технолог молочного производства",
            
            # СТРОИТЕЛЬСТВО И МОНТАЖ
            "инженер-строитель", "монтажник", "монтажник оборудования", 
            "монтажник технологических трубопроводов", "монтажник стальных конструкций",
            
            # ОБЩИЕ ПРОМЫШЛЕННЫЕ ПРОФЕССИИ
            "механик", "электрик", "мастер", "начальник участка", "диспетчер",
            "контролер ОТК", "техник-технолог", "нормировщик", "техник-конструктор",
            "лаборант", "испытатель", "дефектовщик"
        ]

    def _load_exclude_keywords(self) -> List[str]:
        """Загружает ключевые слова для исключения не-промышленных вакансий."""
        return [
            # IT-профессии
            "программист", "разработчик", "developer", "frontend", "backend",
            "fullstack", "software", "приложений", "веб", "web", "ios", "android",
            "qa", "тестировщик", "тестер", "devops", "data science", "data scientist",
            "аналитик данных", "machine learning", "ai", "api", "sql", "python",
            "java", "javascript", "c++", "c#", "php", "ruby", "go", "kotlin",
            
            # Офисные и бизнес-профессии
            "менеджер", "маркетолог", "дизайнер", "копирайтер", "контент-менеджер",
            "сео", "smm", "hr", "рекрутер", "бухгалтер", "экономист", "финансист",
            "юрист", "адвокат", "администратор", "офис-менеджер", "секретарь",
            "помощник", "ассистент", "торговый представитель", "продавец",
            "консультант", "оператор call-центра", "логист", "закупщик",
            
            # Другие не-промышленные сферы
            "врач", "медицинский", "учитель", "преподаватель", "педагог",
            "психолог", "социальный работник", "повар", "официант", "бармен",
            "водитель", "курьер", "грузчик", "охранник", "клининг", "уборщик"
        ]

    def _load_industrial_terms(self) -> Dict[str, List[str]]:
        """Загружает промышленные термины по отраслям."""
        return {
            "automation": [
                'кипиа', 'асу тп', 'контрольно-измерительные приборы', 'автоматизация',
                'scada', 'телемеханика', 'сенсор', 'датчик', 'реле', 'контроллер',
                'исполнительный механизм', 'задвижка', 'клапан', 'регулятор'
            ],
            "metallurgy": [
                'сталь', 'алюминий', 'медь', 'прокат', 'плавка', 'ковш', 'печь',
                'мартен', 'конвертер', 'разливка', 'прокатный стан', 'металлургический'
            ],
            "chemical": [
                'реактор', 'синтез', 'химический', 'реакция', 'катализатор',
                'дистилляция', 'ректификация', 'полимер', 'пластмасса'
            ],
            "energy": [
                'энергия', 'турбина', 'генератор', 'трансформатор', 'подстанция',
                'электроснабжение', 'теплоснабжение', 'котельная', 'энергоблок'
            ],
            "general": [
                'станок', 'оборудование', 'производство', 'цех', 'завод', 'фабрика',
                'механизм', 'деталь', 'чертеж', 'технология', 'сварка', 'токарная',
                'фрезерная', 'литье', 'металл', 'обработка', 'сборка', 'монтаж'
            ]
        }

    def get_industrial_terms_by_industry(self, industry: str = "all") -> List[str]:
        """Возвращает термины для конкретной промышленной отрасли"""
        base_terms = [
            'станок', 'оборудование', 'производство', 'цех', 'завод', 'фабрика',
            'механизм', 'деталь', 'чертеж', 'технология', 'сварка', 'токарная',
            'фрезерная', 'литье', 'металл', 'обработка', 'сборка', 'монтаж'
        ]
        
        if industry == "all":
            all_terms = base_terms
            for terms in self.industrial_terms.values():
                all_terms.extend(terms)
            return list(set(all_terms))
        else:
            return base_terms + self.industrial_terms.get(industry, [])

    def get_vacancies_extended(self, search_text: str, area: int = 113,
                             specialization: Optional[int] = None,
                             date_from: Optional[str] = None,
                             date_to: Optional[str] = None,
                             per_page: int = 50, page: int = 0) -> Dict:
        """
     
        """
        params = {
            "text": search_text,
            "area": area,
            "per_page": per_page,
            "page": page,
            "only_with_salary": False
        }
        
        # Добавляем опциональные параметры согласно промпту
        if specialization:
            params["specialization"] = specialization
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
            
        try:
            self.logger.info(f"Запрос: {search_text}, страница {page}, даты {date_from}-{date_to}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json',
            }
            
            response = requests.get(self.base_url, params=params, timeout=self.config['timeout'], headers=headers)
            
            if response.status_code == 403:
                self.logger.warning(f"🔒 403 Forbidden для запроса '{search_text}'")
                return {}
                
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Получено {len(data.get('items', []))} вакансий")
                return data
            else:
                self.logger.warning(f"HTTP {response.status_code} для '{search_text}'")
                return {}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ошибка при запросе к API: {e}")
            return {}

    def get_vacancies_by_periods(self, search_text: str, 
                                start_date: str, 
                                end_date: str,
                                area: int = 113,
                                specialization: Optional[int] = None,
                                days_per_chunk: int = 30) -> List[Dict]:
        """
      
        """
        all_vacancies = []
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current_date <= end_dt:
            chunk_end = min(current_date + timedelta(days=days_per_chunk), end_dt)
            
            date_from_str = current_date.strftime("%Y-%m-%d")
            date_to_str = chunk_end.strftime("%Y-%m-%d")
            
            self.logger.info(f"Сбор данных за период: {date_from_str} - {date_to_str}")
            
            # Получаем все страницы для этого периода
            page = 0
            while True:
                data = self.get_vacancies_extended(
                    search_text=search_text,
                    area=area,
                    specialization=specialization,
                    date_from=date_from_str,
                    date_to=date_to_str,
                    page=page
                )
                
                if not data or 'items' not in data or not data['items']:
                    break
                    
                all_vacancies.extend(data['items'])
                
                # Проверяем, есть ли следующая страница
                if page >= data['pages'] - 1:
                    break
                    
                page += 1
                time.sleep(self.config['requests_delay'])
                
            current_date = chunk_end + timedelta(days=1)
            
        self.logger.info(f"Всего собрано вакансий за период: {len(all_vacancies)}")
        return all_vacancies

    def get_detailed_vacancy_info(self, vacancy_id: str) -> Dict:
        """
      
        """
        try:
            url = f"{self.base_url}/{vacancy_id}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json',
            }
            response = requests.get(url, timeout=10, headers=headers)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ошибка получения деталей вакансии {vacancy_id}: {e}")
            return {}

    def enrich_vacancies_with_details(self, vacancies: List[Dict]) -> List[Dict]:
        """
     
        """
        enriched_vacancies = []
        
        for i, vacancy in enumerate(vacancies):
            if i % 50 == 0:
                self.logger.info(f"Обогащение вакансий: {i}/{len(vacancies)}")
                
            vacancy_id = vacancy.get('id')
            if vacancy_id:
                details = self.get_detailed_vacancy_info(vacancy_id)
                if details:
                    # Объединяем базовую и детальную информацию
                    enriched_vacancy = {**vacancy, **details}
                    enriched_vacancies.append(enriched_vacancy)
                else:
                    enriched_vacancies.append(vacancy)
                    
                time.sleep(0.5)  # Пауза между запросами
                
        return enriched_vacancies

    def get_industrial_specializations(self) -> List[Dict]:
        """
      
        """
        industrial_specializations = [
            {"id": 7, "name": "Добыча сырья"},
            {"id": 9, "name": "Искусство, развлечения, масс-медиа"},  # Имеет подспециализации
            {"id": 17, "name": "Инсталляция и сервис"},
            {"id": 23, "name": "Маркетинг, реклама, PR"},
            {"id": 29, "name": "Производство"},
            {"id": 33, "name": "Рабочий персонал"},
            {"id": 34, "name": "Розничная торговля"},
            {"id": 36, "name": "Сельское хозяйство"},
            {"id": 41, "name": "Строительство"},
            {"id": 42, "name": "Администрирование"},
            {"id": 45, "name": "Транспорт, логистика"},
            {"id": 48, "name": "Управление персоналом, тренинги"}
        ]
        
        return industrial_specializations

    def is_industrial_vacancy(self, vacancy: Dict) -> bool:
        """Определяет, является ли вакансия промышленной."""
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

    def quick_industrial_filter(self, vacancy: Dict) -> bool:
        """Быстрая фильтрация для массового сбора."""
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
        return any(inc in name for inc in quick_include)

    def search_vacancies(self, query: str, region: int, max_pages: int = 10) -> List[Dict]:
        """Поиск вакансий по запросу и региону с обработкой ошибок 403."""
        vacancies = []
        
        for page in range(max_pages):
            try:
                # Используем расширенный метод с поддержкой дат
                data = self.get_vacancies_extended(query, region, page=page)
                
                if not data or 'items' not in data:
                    break
                    
                page_vacancies = data.get('items', [])
                
                # Фильтрация дубликатов и промышленных вакансий
                filtered = []
                for vacancy in page_vacancies:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and vacancy_id not in self.collected_ids 
                        and self.quick_industrial_filter(vacancy)):
                        filtered.append(vacancy)
                        with self.lock:
                            self.collected_ids.add(vacancy_id)
                
                vacancies.extend(filtered)
                
                # Проверяем наличие следующих страниц
                if not page_vacancies or page >= data.get('pages', 0) - 1:
                    break
                    
                # Увеличиваем задержку между запросами со случайной компонентой
                delay = self.config['requests_delay'] + random.uniform(0.5, 2.0)
                time.sleep(delay)
                
            except Exception as e:
                self.logger.warning(f"Ошибка '{query}' стр {page}: {e}")
                break
        
        return vacancies

    def collect_complete_dataset(self, 
                               search_queries: List[str] = None,
                               start_date: str = None,
                               end_date: str = None,
                               areas: List[int] = None,
                               max_vacancies_per_query: int = 2000) -> Dict:
        """
      
        """
        if search_queries is None:
            search_queries = self.config['industrial_queries']
            
        if areas is None:
            areas = list(self.config['regions'].values())
            
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=self.config['days_back'])).strftime("%Y-%m-%d")
            
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        complete_dataset = {}
        
        self.logger.info(f"🚀 Начинаем сбор полного датасета")
        self.logger.info(f"📅 Период: {start_date} - {end_date}")
        self.logger.info(f"🔍 Запросов: {len(search_queries)}")
        self.logger.info(f"🌍 Регионов: {len(areas)}")
        
        for query in search_queries:
            self.logger.info(f"🔎 Сбор по запросу: '{query}'")
            query_vacancies = []
            
            for area in areas:
                self.logger.info(f"📍 Регион: {area}")
                
                # Используем метод сбора по периодам
                vacancies = self.get_vacancies_by_periods(
                    search_text=query,
                    start_date=start_date,
                    end_date=end_date,
                    area=area
                )
                
                # Фильтруем промышленные вакансии
                industrial_vacancies = [v for v in vacancies if self.is_industrial_vacancy(v)]
                query_vacancies.extend(industrial_vacancies)
                
                self.logger.info(f"✅ Регион {area}: {len(industrial_vacancies)} промышленных вакансий")
                
                # Ограничение на количество вакансий на запрос
                if len(query_vacancies) >= max_vacancies_per_query:
                    self.logger.info(f"🎯 Достигнут лимит вакансий для запроса '{query}'")
                    query_vacancies = query_vacancies[:max_vacancies_per_query]
                    break
                    
            complete_dataset[query] = query_vacancies
            self.logger.info(f"✅ Запрос '{query}': собрано {len(query_vacancies)} вакансий")
            
        return complete_dataset

    def collect_complete_industrial_dataset(self, 
                                         target_count: int = None,
                                         days_back: int = None) -> Dict[str, List[Dict]]:
        """
        Сбор полного датасета промышленных вакансий.
     
        """
        if target_count:
            self.config['target_vacancies'] = target_count
            
        self.logger.info(f"🚀 Сбор полного датасета промышленных вакансий")
        self.logger.info(f"🎯 Цель: {self.config['target_vacancies']} вакансий")
        self.logger.info(f"⚙️ Настройки: {self.config['max_workers']} потоков, задержка {self.config['requests_delay']}с")
        
        start_time = datetime.now()
        all_vacancies = {}
        search_combinations = []
        
        # Создаем комбинации для поиска
        for query in self.config['industrial_queries']:
            for region_name, region_code in self.config['regions'].items():
                search_combinations.append((query, region_name, region_code))
        
        self.logger.info(f"📝 Комбинаций для поиска: {len(search_combinations)}")
        
        # Перемешиваем комбинации для равномерной нагрузки
        random.shuffle(search_combinations)
        
        # Многопоточный сбор
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            # Создаем задачи
            future_to_combo = {
                executor.submit(self.search_vacancies, query, region_code, self.config['max_pages_per_query']): (query, region_name)
                for query, region_name, region_code in search_combinations
            }
            
            # Обрабатываем результаты
            completed = 0
            for future in concurrent.futures.as_completed(future_to_combo):
                query, region_name = future_to_combo[future]
                completed += 1
                
                try:
                    vacancies = future.result()
                    if vacancies:
                        key = f"{query}_{region_name}"
                        all_vacancies[key] = vacancies
                        
                        with self.lock:
                            self.total_collected += len(vacancies)
                    
                    # Логируем прогресс
                    progress_percent = (completed / len(search_combinations)) * 100
                    self.logger.info(f"🔄 Прогресс: {completed}/{len(search_combinations)} ({progress_percent:.1f}%) завершено, собрано {self.total_collected} вакансий")
                        
                    # Проверяем достижение цели
                    if self.total_collected >= self.config['target_vacancies']:
                        self.logger.info(f"🎯 Достигнута цель: {self.total_collected} вакансий")
                        break
                        
                    # Пауза между обработкой результатов
                    time.sleep(1.0)
                        
                except Exception as e:
                    self.logger.error(f"Ошибка в потоке {query}_{region_name}: {e}")
        
        collection_time = datetime.now() - start_time
        
        # Анализируем результаты
        self.analyze_industrial_data(all_vacancies)
        
        self.logger.info(f"⏱️ Время сбора: {collection_time}")
        if collection_time.total_seconds() > 0:
            self.logger.info(f"🚀 Скорость: {self.total_collected/collection_time.total_seconds():.1f} вакансий/сек")
        
        return all_vacancies

    def analyze_industrial_data(self, all_vacancies: Dict[str, List[Dict]]):
        """Анализ собранных промышленных вакансий."""
        if not all_vacancies:
            self.logger.warning("Нет данных для анализа")
            return
            
        total_vacancies = sum(len(v) for v in all_vacancies.values())
        
        self.logger.info("=" * 50)
        self.logger.info("📊 АНАЛИЗ СОБРАННЫХ ДАННЫХ")
        self.logger.info("=" * 50)
        self.logger.info(f"Всего вакансий: {total_vacancies}")
        
        # Статистика по профессиям
        profession_stats = {}
        for key, vacancies in all_vacancies.items():
            profession = key.split('_')[0]
            if profession not in profession_stats:
                profession_stats[profession] = 0
            profession_stats[profession] += len(vacancies)
        
        self.logger.info("\n📈 РАСПРЕДЕЛЕНИЕ ПО ПРОФЕССИЯМ:")
        for profession, count in sorted(profession_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
            percentage = (count / total_vacancies) * 100
            self.logger.info(f"  {profession}: {count} ({percentage:.1f}%)")
        
        # Статистика по зарплатам
        salary_stats = self.analyze_salaries(all_vacancies)
        if salary_stats:
            self.logger.info(f"\n💰 СТАТИСТИКА ПО ЗАРПЛАТАМ:")
            self.logger.info(f"  Вакансий с зарплатой: {salary_stats['with_salary']}/{total_vacancies} ({salary_stats['salary_percentage']:.1f}%)")
            if salary_stats['avg_salary'] > 0:
                self.logger.info(f"  Средняя зарплата: {salary_stats['avg_salary']:,.0f} руб.")

    def analyze_salaries(self, all_vacancies: Dict[str, List[Dict]]) -> Dict[str, float]:
        """Анализ зарплат в собранных вакансиях."""
        total_with_salary = 0
        salary_sum = 0
        salary_count = 0
        
        for vacancies in all_vacancies.values():
            for vacancy in vacancies:
                salary = vacancy.get('salary')
                if salary and (salary.get('from') or salary.get('to')):
                    total_with_salary += 1
                    
                    salary_val = 0
                    if salary.get('from') and salary.get('to'):
                        salary_val = (salary['from'] + salary['to']) / 2
                    elif salary.get('from'):
                        salary_val = salary['from']
                    elif salary.get('to'):
                        salary_val = salary['to'] * 0.8
                    
                    if salary_val > 0:
                        # Конвертируем в рубли если нужно
                        currency = salary.get('currency', 'RUR')
                        if currency == 'USD':
                            salary_val *= 95
                        elif currency == 'EUR':
                            salary_val *= 100
                            
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

    def save_complete_dataset(self, dataset: Dict, base_filename: str = None):
        """
        Сохранение полного датасета в структурированные файлы.
  
        """
        if not base_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"complete_dataset_{timestamp}"
            
        # Создаем директорию для полного датасета
        dataset_dir = f"data/complete/{base_filename}"
        os.makedirs(dataset_dir, exist_ok=True)
        
        total_vacancies = sum(len(vacancies) for vacancies in dataset.values())
        
        # Сохраняем общую статистику
        stats = {
            "total_queries": len(dataset),
            "total_vacancies": total_vacancies,
            "queries": {}
        }
        
        for query, vacancies in dataset.items():
            # Сохраняем вакансии по каждому запросу
            query_filename = f"{dataset_dir}/{query.replace(' ', '_')}.json"
            with open(query_filename, 'w', encoding='utf-8') as f:
                json.dump(vacancies, f, ensure_ascii=False, indent=2)
                
            # Сохраняем статистику по запросу
            stats["queries"][query] = len(vacancies)
            
        # Сохраняем общую статистику
        stats_filename = f"{dataset_dir}/dataset_statistics.json"
        with open(stats_filename, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
            
        # Сохраняем объединенный датасет
        all_vacancies = []
        for vacancies in dataset.values():
            all_vacancies.extend(vacancies)
            
        combined_filename = f"{dataset_dir}/combined_vacancies.json"
        with open(combined_filename, 'w', encoding='utf-8') as f:
            json.dump(all_vacancies, f, ensure_ascii=False, indent=2)
            
        self.logger.info(f"💾 Полный датасет сохранен в: {dataset_dir}")
        self.logger.info(f"📊 Статистика: {stats}")
        
        return dataset_dir


# Функции для интеграции с основной системой
def collect_full_industrial_dataset():
    """Функция для сбора полного датасета по промышленности."""
    client = HHAPIClient()
    
    # Определяем период сбора - последние 2 года
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    # Основные промышленные регионы
    industrial_areas = [113, 1, 2, 66, 76, 1202]  # Россия, Москва, СПб, Екатеринбург, etc.
    
    # Собираем полный датасет используя новый метод
    complete_dataset = client.collect_complete_dataset(
        search_queries=client.config['industrial_queries'],
        start_date=start_date,
        end_date=end_date,
        areas=industrial_areas,
        max_vacancies_per_query=3000
    )
    
    # Сохраняем датасет
    dataset_dir = client.save_complete_dataset(complete_dataset)
    
    print(f"✅ Полный датасет собран и сохранен в: {dataset_dir}")
    return dataset_dir


if __name__ == "__main__":
    collect_full_industrial_dataset()