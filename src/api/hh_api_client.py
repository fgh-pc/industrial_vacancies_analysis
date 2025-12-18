"""
УЛЬТРА-ОПТИМИЗИРОВАННЫЙ КЛИЕНТ ДЛЯ 500,000+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ
ФИНАЛЬНАЯ ВЕРСИЯ - ТОЛЬКО РОССИЙСКИЕ РЕГИОНЫ + РАСШИРЕННЫЕ ПРОМЫШЛЕННЫЕ ПРОФЕССИИ
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
    user_agent: str = "UltraIndustrialCollector/9.0 (pavelkondrov03@mail.ru)"
    max_concurrent_requests: int = 25
    request_timeout: int = 30
    requests_per_minute: int = 120

class UltraIndustrialClient:
    """
    Ультра-оптимизированный клиент для 500,000+ промышленных вакансий.
    ТОЛЬКО российские регионы + расширенные промышленные профессии.
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
            'industries_processed': 0,
            'professional_roles_processed': 0,
            'start_time': time.time(),
            'last_request_time': 0,
            'last_save_time': time.time(),
            'consecutive_empty_regions': 0,  # Счетчик пустых регионов
            'max_consecutive_empty_regions': 10  # Максимум пустых регионов подряд
        }
        
        # Кэш для избежания дубликатов
        self.processed_vacancy_ids: Set[str] = set()
        
        # Промышленные ID
        self.industrial_industry_ids: Set[str] = set()
        self.industrial_professional_role_ids: Set[str] = set()
        
        # Ключевые слова для исключения (НЕпромышленные)
        self.non_industrial_keywords = self._get_non_industrial_keywords()
        
        # Ключевые слова для ВКЛЮЧЕНИЯ (промышленные)
        self.industrial_include_keywords = self._get_industrial_include_keywords()
        
        # Приоритетные российские регионы для сбора
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
        
        # Сверх-продуктивные регионы для дособора
        self.super_productive_regions = [
            'Москва', 'Санкт-Петербург', 'Московская область',
            'Свердловская область', 'Краснодарский край', 'Республика Татарстан',
            'Нижегородская область', 'Челябинская область', 'Красноярский край'
        ]

    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования."""
        logger = logging.getLogger('UltraIndustrialClient')
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
            # Офисные/административные
            'менеджер', 'офис-менеджер', 'секретарь', 'администратор', 'координатор',
            'ассистент', 'помощник', 'референт', 'делопроизводитель', 'архивариус',
            
            # Продажи/маркетинг
            'продавец', 'кассир', 'мерчендайзер', 'торговый представитель', 'супервайзер',
            'маркетолог', 'пиар', 'pr-', 'smm', 'копирайтер', 'контент-менеджер',
            
            # IT (кроме промышленного IT)
            'программист', 'разработчик', 'тестировщик', 'qa', 'devops', 'сисадмин',
            'системный администратор', 'веб-', 'frontend', 'backend', 'fullstack',
            'data scientist', 'аналитик данных', 'гейм-дизайнер',
            
            # Финансы/юриспруденция
            'бухгалтер', 'экономист', 'финансист', 'аудитор', 'юрист', 'адвокат',
            'нотариус', 'следователь',
            
            # HR/рекрутинг
            'рекрутер', 'hr-', 'эйчар', 'кадр', 'специалист по подбору',
            
            # Обслуживание/клининг
            'уборщик', 'уборщица', 'клининг', 'дворник', 'гардеробщик',
            
            # Транспорт/логистика (кроме промышленной)
            'водитель', 'курьер', 'экспедитор', 'логист', 'диспетчер',
            
            # Общепит/гостиницы
            'повар', 'бармен', 'официант', 'бариста', 'пекарь', 'кондитер',
            'горничная', 'хостес',
            
            # Охрана/безопасность
            'охранник', 'вахтер', 'контролер',
            
            # Медицина/красота
            'врач', 'медсестра', 'фельдшер', 'косметолог', 'парикмахер',
            'массажист', 'тренер', 'инструктор',
            
            # Образование
            'преподаватель', 'учитель', 'воспитатель', 'методист',
            
            # Искусство/дизайн
            'дизайнер', 'художник', 'фотограф', 'визажист',
            
            # Другие непромышленные
            'агроном', 'зоотехник', 'ветеринар', 'психолог', 'социолог'
        }

    def _get_industrial_include_keywords(self) -> Set[str]:
        """Ключевые слова для ВКЛЮЧЕНИЯ промышленных вакансий."""
        return {
            # Основные инженерные специальности
            'инженер', 'технолог', 'конструктор', 'механик', 'электрик',
            'энергетик', 'проектировщик', 'техник',
            
            # Производственные рабочие
            'сварщик', 'токарь', 'фрезеровщик', 'слесарь', 'станочник',
            'наладчик', 'оператор', 'аппаратчик', 'машинист',
            
            # КИПиА и АСУ ТП
            'кип', 'кипиа', 'кип и а', 'контрольно-измерительные приборы',
            'приборист', 'асу тп', 'асутп', 'автоматизированные системы',
            'автоматика', 'телемеханик',
            
            # Металлургия и обработка металлов
            'металлург', 'литейщик', 'кузнец', 'волочильщик', 'прокатчик',
            'термист', 'гальваник',
            
            # Химическая промышленность
            'химик', 'лаборант', 'аналитик', 'технолог химик',
            
            # Энергетика
            'электромонтер', 'энергетик', 'электромонтажник', 'электромеханик',
            'релейщик', 'электроэнергетик',
            
            # Строительство и монтаж
            'монтажник', 'строитель', 'каменщик', 'штукатур', 'маляр',
            'кровельщик', 'арматурщик', 'бетонщик',
            
            # Нефтегазовая отрасль
            'нефтяник', 'газовик', 'бурильщик', 'оператор добычи',
            'оператор технологических установок',
            
            # Горная промышленность
            'горняк', 'взрывник', 'проходчик', 'маркшейдер',
            
            # Лесная и деревообрабатывающая
            'лесник', 'деревообработчик', 'столяр', 'плотник',
            
            # Целлюлозно-бумажная
            'бумажник', 'целлюлозник',
            
            # Пищевая промышленность
            'технолог пищевой', 'аппаратчик пищевой', 'оператор линии',
            
            # Текстильная промышленность
            'текстильщик', 'прядильщик', 'ткач',
            
            # Управление и контроль качества
            'метролог', 'контролер качества', 'лаборант химико-бактериологический',
            'бракер',
            
            # Ремонт и обслуживание
            'ремонтник', 'механик по ремонту', 'электрик по ремонту',
            'слесарь-ремонтник',
            
            # Руководство в производстве
            'мастер', 'бригадир', 'начальник участка', 'начальник цеха',
            'производитель работ', 'прораб'
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Создает сессию с авторизацией."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.api_config.request_timeout)
            connector = aiohttp.TCPConnector(limit=400, limit_per_host=200)
            
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
                    elif response.status == 429:
                        self.logger.warning("⚠️ Превышен лимит запросов, ждем 8 секунд...")
                        await asyncio.sleep(8)
                        return await self._send_request(url, params)
                    else:
                        return {}
            except Exception as e:
                self.stats['failed_requests'] += 1
                return {}

    async def get_industrial_industries(self) -> Dict[str, Dict]:
        """Получает промышленные отрасли."""
        url = f"{self.api_config.base_url}/industries"
        data = await self._send_request(url)
        
        industrial_industries = {}
        industrial_keywords = [
            'промышлен', 'производ', 'добыча', 'обработка', 'строитель',
            'энергетик', 'металлург', 'машиностроен', 'химическ', 'нефт',
            'газ', 'уголь', 'лес', 'дерево', 'целлюлоз', 'бумажн',
            'стройматериал', 'автомобил', 'судостроен', 'авиацион',
            'железнодорож', 'оборон', 'воен', 'космическ', 'электрон',
            'радио', 'приборостроен', 'медицинск', 'фармацевт', 'пищев',
            'текстил', 'швейн', 'кожевен', 'обувн', 'полиграф', 'мебель'
        ]
        
        if data:
            for industry in data:
                industry_name = industry.get('name', '').lower()
                industry_id = industry.get('id')
                
                if any(keyword in industry_name for keyword in industrial_keywords):
                    industrial_industries[industry_id] = industry
                    self.industrial_industry_ids.add(industry_id)
        
        self.logger.info(f" Найдено промышленных отраслей: {len(industrial_industries)}")
        return industrial_industries

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
                    
                    # Используем расширенный список ключевых слов
                    if any(keyword in role_name for keyword in self.industrial_include_keywords):
                        industrial_roles[role_id] = role
                        self.industrial_professional_role_ids.add(role_id)
        
        self.logger.info(f" Найдено промышленных ролей: {len(industrial_roles)}")
        return industrial_roles

    async def get_all_russian_regions(self) -> Dict[str, int]:
        """
        ФИКСИРОВАННЫЙ МЕТОД: Получает ТОЛЬКО российские регионы.
        """
        url = f"{self.api_config.base_url}/areas/countries"
        countries_data = await self._send_request(url)
        
        russia_id = None
        regions = {}
        
        # Находим ID России
        if countries_data:
            for country in countries_data:
                if country.get('name') == 'Россия':
                    russia_id = country.get('id')
                    break
        
        if russia_id:
            url = f"{self.api_config.base_url}/areas/{russia_id}"
            russia_data = await self._send_request(url)
            
            if russia_data and 'areas' in russia_data:
                # Собираем все регионы России
                for area in russia_data['areas']:
                    regions[area['name']] = area['id']
                    
                    # Также собираем суб-регионы (города и районы)
                    if 'areas' in area:
                        for sub_area in area['areas']:
                            regions[sub_area['name']] = sub_area['id']
        
        self.logger.info(f"🇷🇺 Российских регионов собрано: {len(regions)}")
        
        # Логируем первые 10 регионов для проверки
        sample_regions = list(regions.keys())[:10]
        self.logger.info(f"📋 Пример регионов: {', '.join(sample_regions)}")
        
        return regions

    async def search_vacancies_by_industry(self, industry_id: str, area_id: int = None, 
                                         page: int = 0, date_from: str = None) -> Dict:
        """Поиск вакансий по ID отрасли."""
        params = {
            "industry": industry_id,
            "per_page": 100,
            "page": page,
            "order_by": "publication_time",
            "enable_snippets": "true"
        }
        
        if area_id:
            params["area"] = area_id
            
        if date_from:
            params["date_from"] = date_from
            
        url = f"{self.api_config.base_url}/vacancies"
        return await self._send_request(url, params)

    async def search_vacancies_by_professional_role(self, role_id: str, area_id: int = None,
                                                  page: int = 0, date_from: str = None) -> Dict:
        """Поиск вакансий по ID профессиональной роли."""
        params = {
            "professional_role": role_id,
            "per_page": 100,
            "page": page,
            "order_by": "publication_time",
            "enable_snippets": "true"
        }
        
        if area_id:
            params["area"] = area_id
            
        if date_from:
            params["date_from"] = date_from
            
        url = f"{self.api_config.base_url}/vacancies"
        return await self._send_request(url, params)

    def _is_true_industrial_vacancy(self, vacancy: Dict) -> bool:
        """
        УЛУЧШЕННАЯ проверка - является ли вакансия промышленной.
        Теперь использует как исключение, так и включение.
        """
        name = vacancy.get('name', '').lower()
        snippet = vacancy.get('snippet', {}).get('requirement', '').lower()
        
        if not name:
            return False
            
        # Сначала проверяем ИСКЛЮЧЕНИЕ - если есть непромышленные ключевые слова
        for exclude_keyword in self.non_industrial_keywords:
            if exclude_keyword in name:
                self.stats['vacancies_filtered_out'] += 1
                return False
        
        # Затем проверяем ВКЛЮЧЕНИЕ - если есть промышленные ключевые слова
        for include_keyword in self.industrial_include_keywords:
            if include_keyword in name or include_keyword in snippet:
                return True
        
        # Если не нашли ни исключающих, ни включающих ключевых слов,
        # проверяем по отраслям и профессиональным ролям
        if vacancy.get('industry') and vacancy['industry'].get('id'):
            if vacancy['industry']['id'] in self.industrial_industry_ids:
                return True
        
        if vacancy.get('professional_roles'):
            for role in vacancy['professional_roles']:
                if role.get('id') in self.industrial_professional_role_ids:
                    return True
        
        # Если не прошли ни одну проверку - отфильтровываем
        self.stats['vacancies_filtered_out'] += 1
        return False

    async def collect_500k_plus_vacancies(self) -> List[Dict]:
        """
        ФИНАЛЬНАЯ ВЕРСИЯ: Собирает 500,000+ промышленных вакансий.
        ТОЛЬКО российские регионы + расширенные промышленные профессии.
        """
        self.logger.info("🚀 ЗАПУСК ФИНАЛЬНОГО СБОРА 500,000+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
        self.logger.info("🇷🇺 ТОЛЬКО РОССИЙСКИЕ РЕГИОНЫ")
        self.logger.info(f"🎯 ЦЕЛЕВОЙ ПОКАЗАТЕЛЬ: {self.target_vacancies:,} вакансий")
        
        # Получаем промышленные ID
        await self.get_industrial_industries()
        await self.get_industrial_professional_roles()
        
        self.logger.info(f"🏭 Промышленные отрасли: {len(self.industrial_industry_ids)}")
        self.logger.info(f"🔧 Промышленные роли: {len(self.industrial_professional_role_ids)}")
        
        # Получаем ТОЛЬКО российские регионы
        all_regions = await self.get_all_russian_regions()
        
        # УВЕЛИЧЕННЫЙ ПЕРИОД СБОРА: 3 года
        date_from = (datetime.now() - timedelta(days=1095)).strftime("%Y-%m-%d")
        all_vacancies = []
        
        self.logger.info("=" * 70)
        self.logger.info("🇷🇺 ФИНАЛЬНАЯ СТРАТЕГИЯ СБОРА - ТОЛЬКО РОССИЯ")
        self.logger.info("🔧 Расширенные промышленные профессии:")
        self.logger.info("   • КИПиА, АСУ ТП, автоматика")
        self.logger.info("   • Все инженерные специальности")
        self.logger.info("   • Производственные рабочие")
        self.logger.info("   • Металлургия, химия, энергетика")
        self.logger.info("=" * 70)
        
        # ОСНОВНОЙ СБОР: ПРИОРИТЕТНЫЕ РЕГИОНЫ
        self.logger.info("🎯 ЭТАП 1: Сбор по приоритетным регионам")
        priority_vacancies = await self._collect_priority_regions(all_regions, date_from)
        all_vacancies.extend(priority_vacancies)
        
        # Проверяем, не достигли ли мы предела доступных вакансий
        if self._should_stop_early():
            self.logger.info("🛑 ДОСТИГНУТ ПРЕДЕЛ ДОСТУПНЫХ ВАКАНСИЙ - завершаем сбор")
            unique_vacancies = self._remove_duplicates(all_vacancies)
            await self._save_500k_plus_results(unique_vacancies)
            return unique_vacancies
        
        # ОСНОВНОЙ СБОР: ОСТАЛЬНЫЕ РЕГИОНЫ
        if self.stats['vacancies_collected'] < self.target_vacancies:
            self.logger.info("🌍 ЭТАП 2: Сбор по остальным регионам России")
            other_vacancies = await self._collect_other_regions(all_regions, date_from)
            all_vacancies.extend(other_vacancies)
            
            # Снова проверяем, не достигли ли мы предела
            if self._should_stop_early():
                self.logger.info("🛑 ДОСТИГНУТ ПРЕДЕЛ ДОСТУПНЫХ ВАКАНСИЙ - завершаем сбор")
                unique_vacancies = self._remove_duplicates(all_vacancies)
                await self._save_500k_plus_results(unique_vacancies)
                return unique_vacancies
        
        # ДОСБОР ДО 500K: СВЕРХ-ПРОДУКТИВНЫЕ РЕГИОНЫ
        if self.stats['vacancies_collected'] < self.target_vacancies:
            self.logger.info("🚀 ЭТАП 3: Ультра-глубокий дособор")
            additional_vacancies = await self._collect_missing_vacancies(all_regions, date_from)
            all_vacancies.extend(additional_vacancies)
        
        # Финальное сохранение
        unique_vacancies = self._remove_duplicates(all_vacancies)
        await self._save_500k_plus_results(unique_vacancies)
        
        return unique_vacancies

    def _should_stop_early(self) -> bool:
        """
        Проверяет, следует ли остановить сбор досрочно.
        Останавливаем, если много регионов подряд не дают вакансий.
        """
        if self.stats['consecutive_empty_regions'] >= self.stats['max_consecutive_empty_regions']:
            self.logger.warning(f"🛑 {self.stats['consecutive_empty_regions']} регионов подряд не дали вакансий")
            self.logger.warning("🛑 Вероятно, достигнут предел доступных промышленных вакансий в России")
            return True
        return False

    async def _collect_priority_regions(self, regions: Dict[str, int], date_from: str) -> List[Dict]:
        """Сбор по приоритетным российским регионам."""
        self.logger.info("🎯 ЗАПУСК СБОРА ПО ПРИОРИТЕТНЫМ РЕГИОНАМ РОССИИ")
        
        vacancies = []
        priority_count = 0
        
        for region_name in self.priority_regions:
            if region_name in regions and self.stats['vacancies_collected'] < self.target_vacancies:
                self.logger.info(f"📍 Обрабатываем приоритетный регион: {region_name}")
                
                region_vacancies = await self._collect_region_ultra_deep(
                    regions[region_name], region_name, date_from, is_priority=True
                )
                
                # Обновляем счетчик пустых регионов
                if len(region_vacancies) == 0:
                    self.stats['consecutive_empty_regions'] += 1
                else:
                    self.stats['consecutive_empty_regions'] = 0
                
                vacancies.extend(region_vacancies)
                self.stats['vacancies_collected'] += len(region_vacancies)
                self.stats['regions_processed'] += 1
                priority_count += 1
                
                self._log_progress()
                
                # Проверяем, не нужно ли остановиться
                if self._should_stop_early():
                    break
        
        self.logger.info(f"✅ Приоритетные регионы завершены: {priority_count} регионов")
        return vacancies

    async def _collect_other_regions(self, regions: Dict[str, int], date_from: str) -> List[Dict]:
        """Сбор по остальным российским регионам."""
        self.logger.info("🌍 ЗАПУСК СБОРА ПО ОСТАЛЬНЫМ РЕГИОНАМ РОССИИ")
        
        vacancies = []
        regions_processed = 0
        
        for region_name, region_id in regions.items():
            if (region_name not in self.priority_regions and 
                self.stats['vacancies_collected'] < self.target_vacancies):
                
                region_vacancies = await self._collect_region_ultra_deep(
                    region_id, region_name, date_from, is_priority=False
                )
                
                # Обновляем счетчик пустых регионов
                if len(region_vacancies) == 0:
                    self.stats['consecutive_empty_regions'] += 1
                else:
                    self.stats['consecutive_empty_regions'] = 0
                
                vacancies.extend(region_vacancies)
                self.stats['vacancies_collected'] += len(region_vacancies)
                self.stats['regions_processed'] += 1
                regions_processed += 1
                
                if self.stats['regions_processed'] % 10 == 0:
                    self._log_progress()
                
                # Проверяем, не нужно ли остановиться
                if self._should_stop_early():
                    self.logger.info(f"🛑 Остановка после обработки {regions_processed} регионов")
                    break
        
        return vacancies

    async def _collect_region_ultra_deep(self, region_id: int, region_name: str, 
                                       date_from: str, is_priority: bool = False) -> List[Dict]:
        """
        УЛЬТРА-ГЛУБОКИЙ сбор в одном регионе.
        ДВОЙНОЙ СБОР: Отрасли + Профессиональные роли.
        """
        vacancies = []
        
        # УВЕЛИЧЕННЫЕ НАСТРОЙКИ ГЛУБИНЫ
        max_pages = 500 if is_priority else 300
        max_concurrent_industries = 10 if is_priority else 6
        
        # ЭТАП 1: СБОР ПО ОТРАСЛЯМ
        self.logger.info(f"  🏭 Сбор по отраслям в {region_name}")
        industry_vacancies = await self._collect_industries_in_region(
            region_id, region_name, date_from, max_pages, max_concurrent_industries
        )
        vacancies.extend(industry_vacancies)
        
        # Быстрое автосохранение
        if len(vacancies) >= 1000:
            await self._auto_save_progress(vacancies)
        
        # ЭТАП 2: СБОР ПО ПРОФЕССИОНАЛЬНЫМ РОЛЯМ (если не достигли цели)
        if (self.stats['vacancies_collected'] < self.target_vacancies and 
            len(self.industrial_professional_role_ids) > 0):
            
            self.logger.info(f"  🔧 Сбор по ролям в {region_name}")
            role_vacancies = await self._collect_roles_in_region(
                region_id, region_name, date_from, max_pages
            )
            vacancies.extend(role_vacancies)
        
        return vacancies

    async def _collect_industries_in_region(self, region_id: int, region_name: str,
                                          date_from: str, max_pages: int, max_concurrent: int) -> List[Dict]:
        """Параллельный сбор по отраслям в регионе."""
        vacancies = []
        industry_tasks = []
        industries_list = list(self.industrial_industry_ids)
        
        for i in range(0, len(industries_list), max_concurrent):
            batch = industries_list[i:i + max_concurrent]
            
            for industry_id in batch:
                if self.stats['vacancies_collected'] >= self.target_vacancies:
                    break
                    
                task = self._collect_industry_pages_deep(
                    industry_id, region_id, region_name, date_from, max_pages
                )
                industry_tasks.append(task)
            
            # Выполняем батч параллельно
            batch_results = await asyncio.gather(*industry_tasks, return_exceptions=True)
            for result in batch_results:
                if isinstance(result, list):
                    vacancies.extend(result)
                    self.stats['vacancies_collected'] += len(result)
            
            industry_tasks = []  # Очищаем для следующего батча
            
        return vacancies

    async def _collect_roles_in_region(self, region_id: int, region_name: str,
                                     date_from: str, max_pages: int) -> List[Dict]:
        """Сбор по профессиональным ролям в регионе."""
        vacancies = []
        role_tasks = []
        
        for role_id in self.industrial_professional_role_ids:
            if self.stats['vacancies_collected'] >= self.target_vacancies:
                break
                
            task = self._collect_role_pages_deep(
                role_id, region_id, region_name, date_from, max_pages
            )
            role_tasks.append(task)
            
            # Ограничиваем параллелизм для ролей
            if len(role_tasks) >= 5:
                batch_results = await asyncio.gather(*role_tasks, return_exceptions=True)
                for result in batch_results:
                    if isinstance(result, list):
                        vacancies.extend(result)
                        self.stats['vacancies_collected'] += len(result)
                role_tasks = []
        
        # Обрабатываем оставшиеся задачи
        if role_tasks:
            batch_results = await asyncio.gather(*role_tasks, return_exceptions=True)
            for result in batch_results:
                if isinstance(result, list):
                    vacancies.extend(result)
                    self.stats['vacancies_collected'] += len(result)
        
        return vacancies

    async def _collect_industry_pages_deep(self, industry_id: str, region_id: int,
                                         region_name: str, date_from: str, max_pages: int) -> List[Dict]:
        """Глубокий сбор по отрасли с пагинацией."""
        vacancies = []
        page = 0
        
        while page < max_pages:
            if self.stats['vacancies_collected'] >= self.target_vacancies:
                break
                
            data = await self.search_vacancies_by_industry(
                industry_id=industry_id,
                area_id=region_id,
                page=page,
                date_from=date_from
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Быстрая фильтрация
            new_vacancies = 0
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'industry'
                        vacancy['industry_id'] = industry_id
                        vacancy['region'] = region_name
                        vacancy['collected_at'] = datetime.now().isoformat()
                        
                        vacancies.append(vacancy)
                        self.processed_vacancy_ids.add(vacancy_id)
                        new_vacancies += 1
                except:
                    continue
            
            # Если на странице не нашли новых вакансий - выходим
            if new_vacancies == 0 and page > 50:
                break
            
            # Проверяем пагинацию
            pages = data.get('pages', 0)
            if page >= pages - 1:
                break
                
            page += 1
        
        return vacancies

    async def _collect_role_pages_deep(self, role_id: str, region_id: int,
                                     region_name: str, date_from: str, max_pages: int) -> List[Dict]:
        """Глубокий сбор по профессиональной роли с пагинацией."""
        vacancies = []
        page = 0
        
        while page < max_pages:
            if self.stats['vacancies_collected'] >= self.target_vacancies:
                break
                
            data = await self.search_vacancies_by_professional_role(
                role_id=role_id,
                area_id=region_id,
                page=page,
                date_from=date_from
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Быстрая фильтрация
            new_vacancies = 0
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'professional_role'
                        vacancy['role_id'] = role_id
                        vacancy['region'] = region_name
                        vacancy['collected_at'] = datetime.now().isoformat()
                        
                        vacancies.append(vacancy)
                        self.processed_vacancy_ids.add(vacancy_id)
                        new_vacancies += 1
                except:
                    continue
            
            # Если на странице не нашли новых вакансий - выходим
            if new_vacancies == 0 and page > 50:
                break
            
            # Проверяем пагинацию
            pages = data.get('pages', 0)
            if page >= pages - 1:
                break
                
            page += 1
        
        return vacancies

    async def _collect_missing_vacancies(self, regions: Dict[str, int], date_from: str) -> List[Dict]:
        """
        УЛЬТРА-ГЛУБОКИЙ дособор недостающих вакансий до 500,000.
        Фокус на самых продуктивных регионах с максимальной глубиной.
        """
        missing = self.target_vacancies - self.stats['vacancies_collected']
        
        if missing <= 0:
            return []
        
        self.logger.info(f"🚀 ДОСБОР: Нужно {missing:,} вакансий до 500,000")
        
        additional_vacancies = []
        
        for region_name in self.super_productive_regions:
            if region_name in regions and missing > 0:
                self.logger.info(f"  🎯 Ультра-глубокий сбор в {region_name}")
                
                # МАКСИМАЛЬНАЯ ГЛУБИНА: 1000 страниц
                region_vacancies = await self._collect_region_mega_deep(
                    regions[region_name], region_name, date_from, 1000
                )
                additional_vacancies.extend(region_vacancies)
                
                missing = self.target_vacancies - self.stats['vacancies_collected']
                self.logger.info(f"  📊 Осталось до цели: {missing:,} вакансий")
                
                if missing <= 0:
                    break
        
        return additional_vacancies

    async def _collect_region_mega_deep(self, region_id: int, region_name: str, 
                                      date_from: str, max_pages: int) -> List[Dict]:
        """МЕГА-ГЛУБОКИЙ сбор в регионе (до 1000 страниц)."""
        vacancies = []
        
        # Параллельный сбор по всем отраслям
        industry_tasks = []
        for industry_id in self.industrial_industry_ids:
            task = self._collect_industry_pages_mega_deep(
                industry_id, region_id, region_name, date_from, max_pages
            )
            industry_tasks.append(task)
        
        industry_results = await asyncio.gather(*industry_tasks, return_exceptions=True)
        for result in industry_results:
            if isinstance(result, list):
                vacancies.extend(result)
                self.stats['vacancies_collected'] += len(result)
        
        # Параллельный сбор по всем ролям
        if self.stats['vacancies_collected'] < self.target_vacancies:
            role_tasks = []
            for role_id in self.industrial_professional_role_ids:
                task = self._collect_role_pages_mega_deep(
                    role_id, region_id, region_name, date_from, max_pages
                )
                role_tasks.append(task)
            
            role_results = await asyncio.gather(*role_tasks, return_exceptions=True)
            for result in role_results:
                if isinstance(result, list):
                    vacancies.extend(result)
                    self.stats['vacancies_collected'] += len(result)
        
        return vacancies

    async def _collect_industry_pages_mega_deep(self, industry_id: str, region_id: int,
                                              region_name: str, date_from: str, max_pages: int) -> List[Dict]:
        """МЕГА-ГЛУБОКИЙ сбор по отрасли (до 1000 страниц)."""
        vacancies = []
        page = 0
        
        while page < max_pages:
            if self.stats['vacancies_collected'] >= self.target_vacancies:
                break
                
            data = await self.search_vacancies_by_industry(
                industry_id=industry_id,
                area_id=region_id,
                page=page,
                date_from=date_from
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Быстрая фильтрация
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'industry_mega'
                        vacancy['industry_id'] = industry_id
                        vacancy['region'] = region_name
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

    async def _collect_role_pages_mega_deep(self, role_id: str, region_id: int,
                                          region_name: str, date_from: str, max_pages: int) -> List[Dict]:
        """МЕГА-ГЛУБОКИЙ сбор по роли (до 1000 страниц)."""
        vacancies = []
        page = 0
        
        while page < max_pages:
            if self.stats['vacancies_collected'] >= self.target_vacancies:
                break
                
            data = await self.search_vacancies_by_professional_role(
                role_id=role_id,
                area_id=region_id,
                page=page,
                date_from=date_from
            )
            
            if not data or 'items' not in data:
                break
                
            items = data.get('items', [])
            if not items:
                break
            
            # Быстрая фильтрация
            for vacancy in items:
                try:
                    vacancy_id = vacancy.get('id')
                    if (vacancy_id and 
                        vacancy_id not in self.processed_vacancy_ids and 
                        self._is_true_industrial_vacancy(vacancy)):
                        
                        vacancy['collection_method'] = 'role_mega'
                        vacancy['role_id'] = role_id
                        vacancy['region'] = region_name
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

    async def _auto_save_progress(self, vacancies: List[Dict]):
        """Автоматическое сохранение прогресса."""
        current_time = time.time()
        if (current_time - self.stats['last_save_time'] > 300 or
            self.stats['vacancies_collected'] % 50000 == 0):
            
            unique_vacancies = self._remove_duplicates(vacancies)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/RUSSIAN_INDUSTRIAL_{len(unique_vacancies)}_{timestamp}.json"
            
            os.makedirs('data', exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(unique_vacancies, f, ensure_ascii=False, indent=1)
            
            self.logger.info(f"💾 Автосохранение: {filename}")
            self.stats['last_save_time'] = current_time

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

    async def _save_500k_plus_results(self, vacancies: List[Dict]):
        """Сохраняет результаты."""
        if not vacancies:
            self.logger.warning("❌ Нет вакансий для сохранения")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/500K_RUSSIAN_INDUSTRIAL_{len(vacancies)}_{timestamp}.json"
        
        os.makedirs('data', exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(vacancies, f, ensure_ascii=False, indent=1)
        
        self.logger.info(f"💾 СОХРАНЕН ФАЙЛ: {filename}")
        
        # Дополнительная статистика
        achievement_percent = (len(vacancies) / self.target_vacancies) * 100
        self.logger.info(f"🎯 Достигнуто: {achievement_percent:.1f}% от цели 500,000")
        self.logger.info(f"🇷🇺 Российских промышленных вакансий: {len(vacancies):,}")

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
            f"🇷🇺 Регионов: {self.stats['regions_processed']} | "
            f"🏭 Отраслей: {self.stats['industries_processed']} | "
            f"🔧 Ролей: {self.stats['professional_roles_processed']} | "
            f"⚡ {rate:.1f} вак/сек | "
            f"⏱️  {hours:.0f}ч {minutes:.0f}м"
        )

    def get_final_stats(self) -> Dict:
        """Возвращает финальную статистику."""
        elapsed = time.time() - self.stats['start_time']
        hours = elapsed // 3600
        minutes = (elapsed % 3600) // 60
        
        return {
            'total_vacancies': self.stats['vacancies_collected'],
            'target_vacancies': self.target_vacancies,
            'achievement_percent': (self.stats['vacancies_collected'] / self.target_vacancies) * 100,
            'filtered_out': self.stats['vacancies_filtered_out'],
            'regions_processed': self.stats['regions_processed'],
            'industries': len(self.industrial_industry_ids),
            'professional_roles': len(self.industrial_professional_role_ids),
            'total_requests': self.stats['total_requests'],
            'time_hours': hours,
            'time_minutes': minutes,
            'vacancies_per_hour': self.stats['vacancies_collected'] / max(elapsed / 3600, 1),
            'early_stop': self.stats['consecutive_empty_regions'] >= self.stats['max_consecutive_empty_regions']
        }

    async def close(self):
        """Закрывает сессию."""
        if self.session:
            await self.session.close()


async def main():
    """Запуск ФИНАЛЬНОГО сбора 500,000+ российских промышленных вакансий."""
    client = UltraIndustrialClient()
    
    try:
        print("=" * 80)
        print("🚀 ФИНАЛЬНЫЙ СБОР 500,000+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
        print("🇷🇺 ТОЛЬКО РОССИЙСКИЕ РЕГИОНЫ")
        print("🔧 РАСШИРЕННЫЕ ПРОМЫШЛЕННЫЕ ПРОФЕССИИ:")
        print("   • КИПиА, АСУ ТП, автоматика")
        print("   • Все инженерные специальности") 
        print("   • Производственные рабочие")
        print("   • Металлургия, химия, энергетика")
        print("🎯 ЦЕЛЕВОЙ ПОКАЗАТЕЛЬ: 500,000+ вакансий")
        print("🛑 АВТО-ОСТАНОВКА: если вакансий меньше 500K")
        print("=" * 80)
        
        start_time = time.time()
        
        # Запускаем ФИНАЛЬНЫЙ сбор
        vacancies = await client.collect_500k_plus_vacancies()
        
        # Статистика
        stats = client.get_final_stats()
        
        print("\n" + "=" * 80)
        print("✅ ФИНАЛЬНЫЙ СБОР ЗАВЕРШЕН!")
        print("=" * 80)
        print(f"🇷🇺 Российских промышленных вакансий: {stats['total_vacancies']:,}")
        print(f"🎯 Достигнуто: {stats['achievement_percent']:.1f}% от цели")
        print(f"❌ Отфильтровано непромышленных: {stats['filtered_out']:,}")
        print(f"🌍 Обработано регионов: {stats['regions_processed']}")
        print(f"📊 Отраслей: {stats['industries']}, Ролей: {stats['professional_roles']}")
        print(f"⏱️  Время: {stats['time_hours']:.0f}ч {stats['time_minutes']:.0f}м")
        print(f"📈 Скорость: {stats['vacancies_per_hour']:.0f} вак/час")
        print(f"📞 Запросов: {stats['total_requests']:,}")
        
        if vacancies:
            print(f"💾 Файл: data/500K_RUSSIAN_INDUSTRIAL_*.json")
        
        if stats['achievement_percent'] >= 100:
            print("🎉 ПОЗДРАВЛЯЕМ! ЦЕЛЬ 500,000+ ДОСТИГНУТА! 🎉")
        elif stats['early_stop']:
            print("🛑 АВТО-ОСТАНОВКА: Достигнут предел доступных промышленных вакансий в России")
            print(f"📈 Максимум доступных вакансий: {stats['total_vacancies']:,}")
        else:
            print(f"📈 Для достижения цели осталось собрать: {500000 - stats['total_vacancies']:,} вакансий")
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Сбор прерван. Собрано: {client.stats['vacancies_collected']:,}")
        # Сохраняем прогресс при прерывании
        if hasattr(client, 'processed_vacancy_ids') and client.processed_vacancy_ids:
            vacancies_list = [{"id": vid} for vid in client.processed_vacancy_ids]
            await client._save_500k_plus_results(vacancies_list)
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