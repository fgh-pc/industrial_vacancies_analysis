"""
СТРАТЕГИЯ СБОРА АКТИВНЫХ ВАКАНСИЙ ЗА 6 МЕСЯЦЕВ
Используем комбинацию методов для обхода ограничений HH.ru
"""

import aiohttp
import asyncio
import time
import json
import os
from typing import Dict, List, Optional, Set
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
import backoff

@dataclass
class APIConfig:
    """Конфигурация API HH.ru."""
    base_url: str = "https://api.hh.ru"
    access_token: str = "APPLJ0H09NIHO3LMSSNUURRFQVEG9IK6I6KHO8E7H5DVDIVVQQC008UIGHOAUCRV"
    user_agent: str = "SixMonthCollector/2.0 (pavelkondrov03@mail.ru)"
    max_concurrent_requests: int = 20
    request_timeout: int = 30
    requests_per_minute: int = 100

class SixMonthIndustrialClient:
    """
    Специализированный клиент для сбора активных вакансий за 6 месяцев.
    Комбинированная стратегия для обхода ограничений.
    """
    
    def __init__(self):
        self.api_config = APIConfig()
        self.session = None
        self.rate_limit_semaphore = asyncio.Semaphore(self.api_config.max_concurrent_requests)
        self.logger = self._setup_logger()
        
        # Целевой показатель
        self.target_vacancies = 500000
        
        # Статистика
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'vacancies_collected': 0,
            'vacancies_filtered_out': 0,
            'regions_processed': 0,
            'professional_roles_processed': 0,
            'start_time': time.time(),
            'last_request_time': 0,
            'last_save_time': time.time(),
            'months_processed': 0,
            'consecutive_empty_regions': 0,
            'max_consecutive_empty_regions': 10
        }
        
        # Кэш для избежания дубликатов
        self.processed_vacancy_ids: Set[str] = set()
        
        # Промышленные профессиональные роли
        self.industrial_professional_role_ids: Set[str] = set()
        
        # Ключевые слова для исключения (НЕпромышленные)
        self.non_industrial_keywords = self._get_non_industrial_keywords()
        
        # Ключевые слова для ВКЛЮЧЕНИЯ (промышленные)
        self.industrial_include_keywords = self._get_industrial_include_keywords()
        
        # Приоритетные российские регионы
        self.priority_regions = [
            'Москва', 'Санкт-Петербург', 'Московская область', 
            'Новосибирская область', 'Свердловская область', 'Краснодарский край',
            'Республика Татарстан', 'Нижегородская область', 'Челябинская область',
            'Республика Башкортостан', 'Красноярский край', 'Самарская область',
            'Пермский край', 'Воронежская область', 'Ростовская область',
            'Иркутская область', 'Омская область', 'Тюменская область',
            'Кемеровская область', 'Волгоградская область', 'Ленинградская область',
            'Архангельская область', 'Вологодская область', 'Калининградская область',
            'Калужская область', 'Курская область', 'Липецкая область',
            'Мурманская область', 'Оренбургская область', 'Пензенская область',
            'Республика Коми', 'Республика Саха', 'Хабаровский край',
            'Ярославская область', 'Томская область', 'Удмуртская Республика'
        ]
        
        # Месяцы для сбора (последние 6 месяцев)
        self.target_months = self._generate_target_months(6)

    def _generate_target_months(self, months_count: int) -> List[str]:
        """Генерирует список месяцев для сбора."""
        months = []
        current_date = datetime.now()
        
        for i in range(months_count):
            target_date = current_date - timedelta(days=30*i)
            month_str = target_date.strftime("%Y-%m")
            months.append(month_str)
            
        self.logger.info(f"📅 Целевые месяцы: {', '.join(months)}")
        return months

    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования."""
        logger = logging.getLogger('SixMonthIndustrialClient')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
        return logger

    def _get_non_industrial_keywords(self) -> Set[str]:
        """Ключевые слова для ИСКЛЮЧЕНИЯ непромышленных вакансий."""
        return {
            'менеджер', 'офис-менеджер', 'секретарь', 'администратор', 'координатор',
            'ассистент', 'помощник', 'референт', 'делопроизводитель',
            'продавец', 'кассир', 'мерчендайзер', 'торговый представитель',
            'маркетолог', 'пиар', 'pr-', 'smm', 'копирайтер', 'контент-менеджер',
            'программист', 'разработчик', 'тестировщик', 'qa', 'devops', 'сисадмин',
            'бухгалтер', 'экономист', 'финансист', 'аудитор', 'юрист', 'адвокат',
            'рекрутер', 'hr-', 'эйчар', 'кадр', 'специалист по подбору',
            'уборщик', 'уборщица', 'клининг', 'дворник',
            'водитель', 'курьер', 'экспедитор', 'логист',
            'повар', 'бармен', 'официант', 'бариста', 'пекарь',
            'охранник', 'вахтер', 'контролер',
            'врач', 'медсестра', 'фельдшер', 'косметолог', 'парикмахер',
            'преподаватель', 'учитель', 'воспитатель',
            'дизайнер', 'художник', 'фотограф', 'визажист'
        }

    def _get_industrial_include_keywords(self) -> Set[str]:
        """Ключевые слова для ВКЛЮЧЕНИЯ промышленных вакансий."""
        return {
            'инженер', 'технолог', 'конструктор', 'механик', 'электрик',
            'энергетик', 'проектировщик', 'техник', 'сварщик', 'токарь',
            'фрезеровщик', 'слесарь', 'станочник', 'наладчик', 'оператор',
            'аппаратчик', 'машинист', 'кип', 'кипиа', 'кип и а', 'приборист',
            'асу тп', 'асутп', 'автоматизированные системы', 'автоматика',
            'телемеханик', 'металлург', 'литейщик', 'кузнец', 'прокатчик',
            'термист', 'гальваник', 'химик', 'лаборант', 'аналитик',
            'электромонтер', 'электромонтажник', 'электромеханик', 'релейщик',
            'монтажник', 'строитель', 'каменщик', 'штукатур', 'маляр',
            'кровельщик', 'арматурщик', 'бетонщик', 'нефтяник', 'газовик',
            'бурильщик', 'горняк', 'взрывник', 'проходчик', 'маркшейдер',
            'лесник', 'деревообработчик', 'столяр', 'плотник', 'бумажник',
            'целлюлозник', 'технолог пищевой', 'аппаратчик пищевой',
            'текстильщик', 'прядильщик', 'ткач', 'метролог', 'контролер качества',
            'бракер', 'ремонтник', 'мастер', 'бригадир', 'начальник участка',
            'начальник цеха', 'производитель работ', 'прораб'
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Создает сессию с авторизацией."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.api_config.request_timeout)
            connector = aiohttp.TCPConnector(limit=300, limit_per_host=150)
            
            headers = {
                'User-Agent': self.api_config.user_agent,
                'Authorization': f'Bearer {self.api_config.access_token}',
                'Accept': 'application/json'
            }
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers
            )
            
        return self.session

    async def _rate_limit(self):
        """Контроль ограничения запросов."""
        current_time = time.time()
        time_since_last = current_time - self.stats['last_request_time']
        
        min_delay = 60.0 / self.api_config.requests_per_minute
        if time_since_last < min_delay:
            await asyncio.sleep(min_delay - time_since_last)
        
        self.stats['last_request_time'] = time.time()

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=15
    )
    async def _send_request(self, url: str, params: Dict = None) -> Dict:
        """Отправляет запрос к API."""
        async with self.rate_limit_semaphore:
            await self._rate_limit()
            session = await self._get_session()
            
            self.stats['total_requests'] += 1
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.stats['successful_requests'] += 1
                        return data
                    elif response.status == 400:
                        return {}
                    elif response.status == 404:
                        return {}
                    elif response.status == 429:
                        self.logger.warning("⚠️ Превышен лимит запросов, ждем 10 секунд...")
                        await asyncio.sleep(10)
                        return await self._send_request(url, params)
                    else:
                        return {}
            except Exception as e:
                self.stats['failed_requests'] += 1
                return {}

    async def get_industrial_professional_roles(self) -> Dict[str, Dict]:
        """Получает промышленные профессиональные роли."""
        url = f"{self.api_config.base_url}/professional_roles"
        data = await self._send_request(url)
        
        industrial_roles = {}
        
        if data and 'categories' in data:
            for category in data['categories']:
                for role in category.get('roles', []):
                    role_name = role.get('name', '').lower()
                    role_id = role.get('id')
                    
                    if any(keyword in role_name for keyword in self.industrial_include_keywords):
                        industrial_roles[role_id] = role
                        self.industrial_professional_role_ids.add(role_id)
        
        self.logger.info(f"🔧 Найдено промышленных ролей: {len(industrial_roles)}")
        return industrial_roles

    async def get_all_russian_regions(self) -> Dict[str, int]:
        """Получает ТОЛЬКО российские регионы."""
        url = f"{self.api_config.base_url}/areas/countries"
        countries_data = await self._send_request(url)
        
        russia_id = None
        regions = {}
        
        if countries_data:
            for country in countries_data:
                if country.get('name') == 'Россия':
                    russia_id = country.get('id')
                    break
        
        if russia_id:
            url = f"{self.api_config.base_url}/areas/{russia_id}"
            russia_data = await self._send_request(url)
            
            if russia_data and 'areas' in russia_data:
                for area in russia_data['areas']:
                    regions[area['name']] = area['id']
        
        self.logger.info(f"🇷🇺 Российских регионов собрано: {len(regions)}")
        return regions

    async def search_vacancies_with_date(self, date_from: str, professional_role: str = None, 
                                       area: int = None, page: int = 0) -> Dict:
        """Поиск вакансий с указанием даты начала периода."""
        params = {
            "date_from": date_from,
            "per_page": 100,
            "page": page,
            "order_by": "publication_time"
        }
        
        if professional_role:
            params["professional_role"] = professional_role
        if area:
            params["area"] = area
            
        url = f"{self.api_config.base_url}/vacancies"
        return await self._send_request(url, params)

    async def search_vacancies_by_text(self, text: str, area: int = None, 
                                     page: int = 0, date_from: str = None) -> Dict:
        """Поиск вакансий по текстовому запросу."""
        params = {
            "text": text,
            "per_page": 100,
            "page": page,
            "order_by": "publication_time"
        }
        
        if area:
            params["area"] = area
        if date_from:
            params["date_from"] = date_from
            
        url = f"{self.api_config.base_url}/vacancies"
        return await self._send_request(url, params)

    def _is_true_industrial_vacancy(self, vacancy: Dict) -> bool:
        """Проверка - является ли вакансия промышленной."""
        name = vacancy.get('name', '').lower()
        snippet = vacancy.get('snippet', {}).get('requirement', '').lower()
        
        if not name:
            return False
            
        # Исключение непромышленных
        for exclude_keyword in self.non_industrial_keywords:
            if exclude_keyword in name:
                self.stats['vacancies_filtered_out'] += 1
                return False
        
        # Включение промышленных
        for include_keyword in self.industrial_include_keywords:
            if include_keyword in name or include_keyword in snippet:
                return True
        
        # Проверка по ролям
        if vacancy.get('professional_roles'):
            for role in vacancy['professional_roles']:
                if role.get('id') in self.industrial_professional_role_ids:
                    return True
        
        self.stats['vacancies_filtered_out'] += 1
        return False

    async def collect_six_month_vacancies(self) -> List[Dict]:
        """
        ОСНОВНОЙ МЕТОД: Собирает активные вакансии за 6 месяцев.
        Комбинированная стратегия для максимального охвата.
        """
        self.logger.info("🚀 ЗАПУСК СБОРА АКТИВНЫХ ВАКАНСИЙ ЗА 6 МЕСЯЦЕВ")
        self.logger.info(f"📅 Период: {self.target_months[0]} - {self.target_months[-1]}")
        self.logger.info(f"🎯 Цель: {self.target_vacancies:,} вакансий")
        
        # Получаем промышленные роли
        await self.get_industrial_professional_roles()
        self.logger.info(f"🔧 Промышленные роли: {len(self.industrial_professional_role_ids)}")
        
        # Получаем регионы
        all_regions = await self.get_all_russian_regions()
        
        all_vacancies = []
        
        self.logger.info("=" * 70)
        self.logger.info("🇷🇺 КОМБИНИРОВАННАЯ СТРАТЕГИЯ СБОРА")
        self.logger.info("1. По профессиональным ролям + дата")
        self.logger.info("2. По ключевым словам + дата") 
        self.logger.info("3. Глубокий поиск по регионам")
        self.logger.info("=" * 70)
        
        # ЭТАП 1: СБОР ПО ПРОФЕССИОНАЛЬНЫМ РОЛЯМ С ДАТАМИ
        self.logger.info("🔧 ЭТАП 1: Сбор по ролям с датами")
        role_vacancies = await self._collect_by_roles_with_dates(all_regions)
        all_vacancies.extend(role_vacancies)
        
        self.logger.info(f"📊 Собрано по ролям: {len(role_vacancies):,}")
        
        if self.stats['vacancies_collected'] >= self.target_vacancies:
            self.logger.info("🎯 ЦЕЛЬ ДОСТИГНУТА!")
            return self._remove_duplicates(all_vacancies)
        
        # ЭТАП 2: СБОР ПО КЛЮЧЕВЫМ СЛОВАМ
        self.logger.info("🔍 ЭТАП 2: Сбор по ключевым словам")
        keyword_vacancies = await self._collect_by_keywords(all_regions)
        all_vacancies.extend(keyword_vacancies)
        
        self.logger.info(f"📊 Собрано по ключевым словам: {len(keyword_vacancies):,}")
        
        if self.stats['vacancies_collected'] >= self.target_vacancies:
            self.logger.info("🎯 ЦЕЛЬ ДОСТИГНУТА!")
            return self._remove_duplicates(all_vacancies)
        
        # ЭТАП 3: ГЛУБОКИЙ СБОР ПО РЕГИОНАМ
        self.logger.info("🌍 ЭТАП 3: Глубокий сбор по регионам")
        region_vacancies = await self._collect_deep_by_regions(all_regions)
        all_vacancies.extend(region_vacancies)
        
        # Финальное сохранение
        unique_vacancies = self._remove_duplicates(all_vacancies)
        await self._save_results(unique_vacancies)
        
        return unique_vacancies

    async def _collect_by_roles_with_dates(self, regions: Dict[str, int]) -> List[Dict]:
        """Сбор по профессиональным ролям с указанием дат."""
        vacancies = []
        tasks = []
        
        # Используем разные даты начала для обхода ограничений
        date_periods = [
            (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),  # 6 месяцев
            (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),   # 3 месяца
            (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),   # 1 месяц
        ]
        
        for region_name in self.priority_regions[:25]:  # 25 приоритетных регионов
            if region_name in regions:
                for role_id in list(self.industrial_professional_role_ids)[:20]:  # 20 основных ролей
                    for date_from in date_periods:
                        task = self._collect_role_with_date(
                            regions[region_name], region_name, role_id, date_from
                        )
                        tasks.append(task)
        
        # Обрабатываем задачи батчами
        batch_size = 12
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, list):
                    vacancies.extend(result)
                    self.stats['vacancies_collected'] += len(result)
            
            self._log_progress()
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)
        
        return vacancies

    async def _collect_role_with_date(self, region_id: int, region_name: str, 
                                    role_id: str, date_from: str) -> List[Dict]:
        """Сбор вакансий для роли с указанием даты."""
        vacancies = []
        page = 0
        max_pages = 50
        
        while page < max_pages:
            data = await self.search_vacancies_with_date(
                date_from=date_from,
                professional_role=role_id,
                area=region_id,
                page=page
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Фильтрация
            new_vacancies = 0
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'role_with_date'
                        vacancy['role_id'] = role_id
                        vacancy['region'] = region_name
                        vacancy['date_from'] = date_from
                        vacancy['collected_at'] = datetime.now().isoformat()
                        
                        vacancies.append(vacancy)
                        self.processed_vacancy_ids.add(vacancy_id)
                        new_vacancies += 1
                except:
                    continue
            
            if new_vacancies == 0:
                break
            
            # Проверяем пагинацию
            pages = data.get('pages', 0)
            if page >= pages - 1:
                break
                
            page += 1
        
        return vacancies

    async def _collect_by_keywords(self, regions: Dict[str, int]) -> List[Dict]:
        """Сбор по ключевым словам промышленности."""
        vacancies = []
        tasks = []
        
        # Ключевые слова для поиска
        keywords = [
            'инженер', 'сварщик', 'токарь', 'фрезеровщик', 'слесарь',
            'оператор станков', 'механик', 'электрик', 'технолог',
            'наладчик', 'кип', 'асу тп', 'монтажник', 'строитель'
        ]
        
        date_from = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        
        for region_name in self.priority_regions[:15]:
            if region_name in regions:
                for keyword in keywords:
                    task = self._collect_keyword_region(
                        regions[region_name], region_name, keyword, date_from
                    )
                    tasks.append(task)
        
        # Обрабатываем задачи батчами
        batch_size = 10
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, list):
                    vacancies.extend(result)
                    self.stats['vacancies_collected'] += len(result)
            
            self._log_progress()
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)
        
        return vacancies

    async def _collect_keyword_region(self, region_id: int, region_name: str, 
                                    keyword: str, date_from: str) -> List[Dict]:
        """Сбор по ключевому слову в регионе."""
        vacancies = []
        page = 0
        max_pages = 30
        
        while page < max_pages:
            data = await self.search_vacancies_by_text(
                text=keyword,
                area=region_id,
                page=page,
                date_from=date_from
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Фильтрация
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'keyword_search'
                        vacancy['keyword'] = keyword
                        vacancy['region'] = region_name
                        vacancy['date_from'] = date_from
                        vacancy['collected_at'] = datetime.now().isoformat()
                        
                        vacancies.append(vacancy)
                        self.processed_vacancy_ids.add(vacancy_id)
                except:
                    continue
            
            # Проверяем пагинацию
            pages = data.get('pages', 0)
            if page >= pages - 1:
                break
                
            page += 1
        
        return vacancies

    async def _collect_deep_by_regions(self, regions: Dict[str, int]) -> List[Dict]:
        """Глубокий сбор по всем регионам."""
        vacancies = []
        
        date_from = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        
        for region_name, region_id in regions.items():
            if self.stats['vacancies_collected'] >= self.target_vacancies:
                break
                
            region_vacancies = await self._collect_region_deep_search(region_id, region_name, date_from)
            vacancies.extend(region_vacancies)
            
            self.stats['regions_processed'] += 1
            
            if len(region_vacancies) == 0:
                self.stats['consecutive_empty_regions'] += 1
            else:
                self.stats['consecutive_empty_regions'] = 0
            
            if self.stats['regions_processed'] % 10 == 0:
                self._log_progress()
            
            if self._should_stop_early():
                break
        
        return vacancies

    async def _collect_region_deep_search(self, region_id: int, region_name: str, date_from: str) -> List[Dict]:
        """Глубокий поиск в регионе."""
        vacancies = []
        
        # Поиск без фильтров по ролям, только по дате
        page = 0
        max_pages = 20
        
        while page < max_pages:
            data = await self.search_vacancies_with_date(
                date_from=date_from,
                area=region_id,
                page=page
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Фильтрация промышленных вакансий
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'region_deep_search'
                        vacancy['region'] = region_name
                        vacancy['date_from'] = date_from
                        vacancy['collected_at'] = datetime.now().isoformat()
                        
                        vacancies.append(vacancy)
                        self.processed_vacancy_ids.add(vacancy_id)
                except:
                    continue
            
            # Проверяем пагинацию
            pages = data.get('pages', 0)
            if page >= pages - 1:
                break
                
            page += 1
        
        return vacancies

    def _should_stop_early(self) -> bool:
        """Проверяет, следует ли остановить сбор досрочно."""
        if self.stats['consecutive_empty_regions'] >= self.stats['max_consecutive_empty_regions']:
            self.logger.warning(f"🛑 {self.stats['consecutive_empty_regions']} регионов подряд не дали вакансий")
            return True
        return False

    def _remove_duplicates(self, vacancies: List[Dict]) -> List[Dict]:
        """Удаляет дубликаты."""
        seen_ids = set()
        unique_vacancies = []
        
        for vacancy in vacancies:
            vacancy_id = vacancy.get('id')
            if vacancy_id and vacancy_id not in seen_ids:
                seen_ids.add(vacancy_id)
                unique_vacancies.append(vacancy)
        
        return unique_vacancies

    async def _save_results(self, vacancies: List[Dict]):
        """Сохраняет результаты."""
        if not vacancies:
            self.logger.warning("❌ Нет вакансий для сохранения")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/SIX_MONTH_INDUSTRIAL_{len(vacancies)}_{timestamp}.json"
        
        os.makedirs('data', exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(vacancies, f, ensure_ascii=False, indent=1)
        
        self.logger.info(f"💾 СОХРАНЕН ФАЙЛ: {filename}")
        
        # Анализ периодов
        dates = []
        for vacancy in vacancies[:1000]:
            published_at = vacancy.get('published_at')
            if published_at:
                try:
                    date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    dates.append(date)
                except:
                    continue
        
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            days_span = (max_date - min_date).days
            self.logger.info(f"📅 Период данных: {min_date.strftime('%Y-%m-%d')} - {max_date.strftime('%Y-%m-%d')} ({days_span} дней)")

    def _log_progress(self):
        """Логирует прогресс."""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['vacancies_collected'] / max(elapsed, 1)
        hours = elapsed // 3600
        minutes = (elapsed % 3600) // 60
        
        progress_percent = (self.stats['vacancies_collected'] / self.target_vacancies) * 100
        
        self.logger.info(
            f"📊 {self.stats['vacancies_collected']:,}/{self.target_vacancies:,} "
            f"({progress_percent:.1f}%) | "
            f"🌍 Регионов: {self.stats['regions_processed']} | "
            f"🔧 Ролей: {len(self.industrial_professional_role_ids)} | "
            f"⚡ {rate:.1f} вак/сек | "
            f"⏱️  {hours:.0f}ч {minutes:.0f}м"
        )

    async def close(self):
        """Закрывает сессию."""
        if self.session:
            await self.session.close()


async def main():
    """Запуск сбора активных вакансий за 6 месяцев."""
    client = SixMonthIndustrialClient()
    
    try:
        print("=" * 80)
        print("🚀 СБОР АКТИВНЫХ ПРОМЫШЛЕННЫХ ВАКАНСИЙ ЗА 6 МЕСЯЦЕВ")
        print("📅 ПЕРИОД: последние 6 месяцев")
        print("🔧 СТРАТЕГИЯ: Комбинированный подход")
        print("🎯 ЦЕЛЬ: 500,000+ вакансий")
        print("🌍 РЕГИОНЫ: Приоритетные регионы России")
        print("=" * 80)
        
        start_time = time.time()
        
        vacancies = await client.collect_six_month_vacancies()
        
        elapsed = time.time() - start_time
        hours = elapsed // 3600
        minutes = (elapsed % 3600) // 60
        
        print("\n" + "=" * 80)
        print("✅ СБОР ЗАВЕРШЕН!")
        print("=" * 80)
        print(f"📅 Собрано вакансий: {len(vacancies):,}")
        print(f"🎯 Достигнуто: {(len(vacancies) / 500000) * 100:.1f}% от цели")
        print(f"🌍 Обработано регионов: {client.stats['regions_processed']}")
        print(f"⏱️  Время: {hours:.0f}ч {minutes:.0f}м")
        print(f"📈 Скорость: {len(vacancies) / max(elapsed / 3600, 1):.0f} вак/час")
        
        if vacancies:
            print(f"💾 Файл: data/SIX_MONTH_INDUSTRIAL_*.json")
        
        if len(vacancies) >= 500000:
            print("🎉 ЦЕЛЬ 500,000+ ВАКАНСИЙ ДОСТИГНУТА! 🎉")
        else:
            print(f"📈 Осталось до цели: {500000 - len(vacancies):,} вакансий")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Сбор прерван. Собрано: {client.stats['vacancies_collected']:,}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()


if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    asyncio.run(main())