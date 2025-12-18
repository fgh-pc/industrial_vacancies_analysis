"""
ОБНОВЛЕННЫЙ МЕНЕДЖЕР БАЗЫ ДАННЫХ ДЛЯ 500K+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ
УПРОЩЕННАЯ ФИЛЬТРАЦИЯ - ДАННЫЕ УЖЕ ПРОМЫШЛЕННЫЕ
"""

import sqlite3
import os
import json
import pandas as pd
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime
import hashlib
import time
import sys

# Добавляем корневую директорию в путь для импорта classification_config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
try:
    from classification_config import classify_industry_segment, classify_position_level
    USE_IMPORTED_CLASSIFIERS = True
except ImportError:
    USE_IMPORTED_CLASSIFIERS = False

class IndustrialDatabaseManager:
    """
    Оптимизированный менеджер БД для работы с 500K+ промышленных вакансий.
    Упрощенная фильтрация для предварительно отфильтрованных данных.
    """
    
    def __init__(self, db_path: str = "industrial_vacancies.db"):
        self.db_path = db_path
        self.connection = None
        self.logger = self._setup_logger()
        self.batch_size = 1000  # Размер батча для массовой вставки
        self.processed_vacancy_ids = set()  # Для отслеживания дубликатов
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования."""
        logger = logging.getLogger('IndustrialDatabaseManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger

    def create_connection(self) -> bool:
        """
        Создает соединение с SQLite с оптимизациями для больших данных.
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            
            # Включаем оптимизации для больших объемов данных
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
            self.connection.execute("PRAGMA cache_size = -64000")  # 64MB кэш
            self.connection.execute("PRAGMA temp_store = MEMORY")
            self.connection.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap
            self.connection.execute("PRAGMA optimize")
            
            self.connection.row_factory = sqlite3.Row
            self.logger.info(f"✅ Подключение к базе данных {self.db_path} установлено")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка подключения к базе данных: {e}")
            return False

    def create_tables(self) -> bool:
        """
        Создает таблицы из SQL файла.
        """
        try:
            sql_file_path = 'data/sql/create_tables.sql'
            if not os.path.exists(sql_file_path):
                # Создаем базовые таблицы если файл не существует
                self.logger.info("📝 Создаем таблицы с базовым SQL...")
                return self._create_basic_tables()
                
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
                
            cursor = self.connection.cursor()
            cursor.executescript(sql_script)
            self.connection.commit()
            
            self.logger.info("✅ Таблицы успешно созданы")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при создании таблиц: {e}")
            # Пробуем создать базовые таблицы
            return self._create_basic_tables()

    def _create_basic_tables(self) -> bool:
        """
        Создает базовые таблицы если основной SQL файл недоступен.
        """
        try:
            cursor = self.connection.cursor()
            
            # Основная таблица вакансий
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    id INTEGER PRIMARY KEY,
                    hh_id TEXT UNIQUE,
                    name TEXT NOT NULL,
                    name_cleaned TEXT,
                    area TEXT,
                    area_id INTEGER,
                    region TEXT,
                    salary_from INTEGER,
                    salary_to INTEGER,
                    salary_currency TEXT,
                    salary_avg_rub INTEGER,
                    experience TEXT,
                    schedule TEXT,
                    employment TEXT,
                    employer_name TEXT,
                    employer_id TEXT,
                    employer_trusted INTEGER DEFAULT 0,
                    industry_segment TEXT,
                    position_level TEXT,
                    professional_roles TEXT,
                    industrial_keywords TEXT,
                    key_skills_json TEXT,
                    published_at TIMESTAMP,
                    created_at TIMESTAMP,
                    collected_at TIMESTAMP,
                    collection_method TEXT,
                    collection_source TEXT,
                    snippet_requirement TEXT,
                    snippet_responsibility TEXT,
                    has_salary INTEGER DEFAULT 0,
                    is_industrial INTEGER DEFAULT 1
                )
            """)
            
            # Таблица навыков
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS skills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vacancy_id INTEGER,
                    skill_name TEXT,
                    skill_category TEXT,
                    frequency_rank INTEGER,
                    FOREIGN KEY (vacancy_id) REFERENCES vacancies (id)
                )
            """)
            
            # Создаем основные индексы
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vacancies_industrial ON vacancies(is_industrial)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vacancies_region ON vacancies(region)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_vacancies_industry_segment ON vacancies(industry_segment)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_skills_vacancy_id ON skills(vacancy_id)")
            
            self.connection.commit()
            self.logger.info("✅ Базовые таблицы созданы")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при создании базовых таблиц: {e}")
            return False

    def _check_tables_exist(self) -> bool:
        """Проверяет существование основных таблиц."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vacancies'")
            return cursor.fetchone() is not None
        except:
            return False

    def _is_true_industrial_vacancy(self, vacancy: Dict) -> bool:
        """
        УПРОЩЕННАЯ проверка для предварительно отфильтрованных данных.
        Поскольку JSON уже содержит промышленные вакансии, фильтрация минимальная.
        """
        # Базовые проверки
        if not vacancy.get('id'):
            return False
            
        name = vacancy.get('name', '')
        if not name:
            return False
        
        # ВАЖНО: Поскольку файл уже содержит промышленные вакансии,
        # мы используем минимальную фильтрацию только для явно непромышленных
        
        name_lower = name.lower()
        
        # Только явно непромышленные категории
        strong_non_industrial = {
            'менеджер по продажам', 'торговый представитель', 'маркетолог',
            'бухгалтер', 'юрист', 'адвокат', 'нотариус',
            'программист', 'разработчик', 'тестировщик', 'айти',
            'секретарь', 'офис-менеджер', 'администратор',
            'официант', 'повар', 'бармен', 'бариста',
            'водитель', 'курьер', 'экспедитор',
            'уборщик', 'уборщица', 'клининг',
            'охранник', 'сторож', 'контролер',
            'продавец', 'кассир', 'консультант',
            'медсестра', 'врач', 'фельдшер',
            'учитель', 'преподаватель', 'воспитатель'
        }
        
        # Проверяем только на явно непромышленные
        for exclude_keyword in strong_non_industrial:
            if exclude_keyword in name_lower:
                return False
        
        # ВСЕ остальные вакансии считаем промышленными
        # поскольку исходный файл уже отфильтрован
        return True

    def load_industrial_data_from_json(self, json_file_path: str) -> int:
        """
        Загружает данные из FINAL_MERGED_INDUSTRIAL_VACANCIES.json в БД.
        Оптимизированная версия для больших файлов с упрощенной фильтрацией.
        """
        try:
            self.logger.info(f"📥 Загрузка данных из {json_file_path}...")
            
            # Проверяем размер файла
            if os.path.exists(json_file_path):
                file_size = os.path.getsize(json_file_path) / (1024 * 1024)  # MB
                self.logger.info(f"📁 Размер файла: {file_size:.1f} MB")
            else:
                self.logger.error(f"❌ Файл {json_file_path} не найден")
                return 0
            
            # Загружаем данные с прогресс-баром
            self.logger.info("🔄 Чтение JSON файла...")
            start_time = time.time()
            
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            load_time = time.time() - start_time
            self.logger.info(f"✅ JSON прочитан за {load_time:.1f} секунд")
    
            if not isinstance(data, list):
                self.logger.error("❌ JSON файл должен содержать список вакансий")
                return 0
            
            total_vacancies = len(data)
            self.logger.info(f"📊 Найдено {total_vacancies:,} вакансий в файле")
            
            # ДИАГНОСТИКА: анализируем данные перед загрузкой
            self._analyze_data_before_load(data)
            
            # Создаем таблицы если их нет
            if not self._check_tables_exist():
                self.logger.info("🔄 Создаем таблицы...")
                if not self.create_tables():
                    self.logger.error("❌ Не удалось создать таблицы")
                    return 0
            
            # Вставляем данные батчами
            total_inserted = self.insert_vacancies_batch(data)
            
            # ДИАГНОСТИКА: проверяем результат загрузки
            self._analyze_load_results(total_vacancies, total_inserted)
            
            self.logger.info(f"✅ Загружено {total_inserted:,} вакансий в базу данных")
            
            # Создаем дополнительные индексы после загрузки
            self._create_additional_indexes()
            
            return total_inserted
            
        except KeyboardInterrupt:
            self.logger.info("⏹️ Загрузка прервана пользователем")
            return 0
        except Exception as e:
            self.logger.error(f"❌ Ошибка при загрузке данных из JSON: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return 0

    def _analyze_data_before_load(self, data: List[Dict]):
        """Анализирует данные перед загрузкой для диагностики."""
        try:
            self.logger.info("🔍 АНАЛИЗ ДАННЫХ ПЕРЕД ЗАГРУЗКОЙ:")
            
            # Проверяем структуру первых нескольких вакансий
            sample_vacancy = data[0] if data else {}
            self.logger.info(f"  📋 Пример вакансии: ID={sample_vacancy.get('id')}, Name={sample_vacancy.get('name')[:50]}...")
            
            # Считаем промышленные vs непромышленные с упрощенной фильтрацией
            industrial_count = 0
            non_industrial_count = 0
            has_salary_count = 0
            
            for i, vacancy in enumerate(data[:1000]):  # Проверяем только первую 1000 для скорости
                if self._is_true_industrial_vacancy(vacancy):
                    industrial_count += 1
                else:
                    non_industrial_count += 1
                
                if vacancy.get('salary'):
                    has_salary_count += 1
            
            self.logger.info(f"  🏭 Промышленные вакансии (упрощенная фильтрация): {industrial_count}/1000")
            self.logger.info(f"  🚫 Отфильтровано (явно непромышленные): {non_industrial_count}/1000")
            self.logger.info(f"  💰 С зарплатой (выборка): {has_salary_count}/1000")
            
            # Проверяем уникальность ID
            ids = [v.get('id') for v in data if v.get('id')]
            unique_ids = set(ids)
            self.logger.info(f"  🔑 Уникальных ID: {len(unique_ids):,} из {len(ids):,}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка анализа данных: {e}")

    def _analyze_load_results(self, total_vacancies: int, inserted_count: int):
        """Анализирует результаты загрузки."""
        self.logger.info("📊 АНАЛИЗ РЕЗУЛЬТАТОВ ЗАГРУЗКИ:")
        self.logger.info(f"  📁 В файле: {total_vacancies:,} вакансий")
        self.logger.info(f"  💾 Загружено: {inserted_count:,} вакансий")
        
        if total_vacancies > 0:
            success_rate = (inserted_count / total_vacancies) * 100
            self.logger.info(f"  📈 Успешность загрузки: {success_rate:.1f}%")
            
            if success_rate < 80:
                self.logger.warning("  ⚠️ Возможные причины расхождений:")
                self.logger.warning("    • Дубликаты вакансий")
                self.logger.warning("    • Явно непромышленные вакансии отфильтрованы")
                self.logger.warning("    • Ошибки формата данных")

    def insert_vacancies_batch(self, vacancies: List[Dict]) -> int:
        """
        Массовая вставка вакансий с упрощенной фильтрацией.
        """
        if not vacancies:
            self.logger.warning("⚠️ Нет вакансий для вставки")
            return 0
            
        inserted_count = 0
        total_vacancies = len(vacancies)
        
        self.logger.info(f"🔄 Начинаем вставку {total_vacancies:,} вакансий...")
        self.logger.info("💡 ИСПОЛЬЗУЕМ УПРОЩЕННУЮ ФИЛЬТРАЦИЮ (данные уже промышленные)")
        
        # Сбрасываем множество обработанных ID для новой загрузки
        self.processed_vacancy_ids.clear()
        
        try:
            cursor = self.connection.cursor()
            
            # Начинаем транзакцию для быстрой вставки
            cursor.execute("BEGIN TRANSACTION")
            
            for i, vacancy in enumerate(vacancies):
                try:
                    # Пропускаем вакансии без ID
                    if not vacancy.get('id'):
                        continue
                    
                    # УПРОЩЕННАЯ ПРОВЕРКА: только базовые проверки
                    vacancy_id = self._generate_vacancy_id(vacancy)
                    if vacancy_id in self.processed_vacancy_ids:
                        continue  # Пропускаем дубликаты
                    
                    # Проверяем с упрощенной фильтрацией
                    if not self._is_true_industrial_vacancy(vacancy):
                        continue
                    
                    # Подготавливаем данные (все вакансии считаем промышленными)
                    vacancy_data = self._prepare_vacancy_data(vacancy)
                    
                    # Вставляем вакансию
                    cursor.execute("""
                        INSERT OR IGNORE INTO vacancies (
                            id, hh_id, name, name_cleaned, area, area_id, region,
                            salary_from, salary_to, salary_currency, salary_avg_rub,
                            experience, schedule, employment, employer_name, employer_id,
                            employer_trusted, industry_segment, position_level,
                            professional_roles, industrial_keywords, key_skills_json,
                            published_at, created_at, collected_at, collection_method,
                            snippet_requirement, snippet_responsibility, has_salary, is_industrial
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, vacancy_data)
                    
                    inserted_count += 1
                    self.processed_vacancy_ids.add(vacancy_id)
                    
                    # Вставляем навыки если они есть
                    if vacancy.get('key_skills'):
                        self._insert_skills_batch(cursor, vacancy_data[0], vacancy['key_skills'])
                    
                    # Логируем прогресс каждые 5000 вакансий
                    if inserted_count % 5000 == 0:
                        progress = (inserted_count / total_vacancies) * 100
                        self.logger.info(f"📊 Прогресс: {inserted_count:,}/{total_vacancies:,} ({progress:.1f}%)")
                        
                    # Коммитим батчами для оптимизации
                    if inserted_count % self.batch_size == 0:
                        self.connection.commit()
                        cursor.execute("BEGIN TRANSACTION")
                        
                except sqlite3.IntegrityError:
                    continue  # Пропускаем дубликаты
                except Exception as e:
                    if inserted_count % 1000 == 0:  # Логируем не все ошибки
                        self.logger.warning(f"⚠️ Ошибка при вставке вакансии {vacancy.get('id')}: {e}")
                    continue
            
            # Финальный коммит
            self.connection.commit()
            self.logger.info(f"✅ Успешно вставлено {inserted_count:,} вакансий")
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"❌ Ошибка при массовой вставке: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            
        return inserted_count

    def insert_vacancy(self, vacancy: Dict) -> bool:
        """
        Обертка для вставки одной вакансии. Возвращает True, если вставка прошла без ошибок.
        """
        try:
            return self.insert_vacancies_batch([vacancy]) > 0
        except Exception as e:
            self.logger.error(f"❌ Ошибка вставки вакансии: {e}")
            return False

    def _create_additional_indexes(self):
        """Создает дополнительные индексы для оптимизации запросов."""
        try:
            cursor = self.connection.cursor()
            
            self.logger.info("🔧 Создаем дополнительные индексы...")
            
            # Индексы для аналитических запросов
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_vacancies_salary_avg ON vacancies(salary_avg_rub)",
                "CREATE INDEX IF NOT EXISTS idx_vacancies_published_at ON vacancies(published_at)",
                "CREATE INDEX IF NOT EXISTS idx_vacancies_position_level ON vacancies(position_level)",
                "CREATE INDEX IF NOT EXISTS idx_vacancies_employer ON vacancies(employer_name)",
                "CREATE INDEX IF NOT EXISTS idx_vacancies_experience ON vacancies(experience)",
                "CREATE INDEX IF NOT EXISTS idx_vacancies_has_salary ON vacancies(has_salary)",
                "CREATE INDEX IF NOT EXISTS idx_skills_skill_name ON skills(skill_name)",
                "CREATE INDEX IF NOT EXISTS idx_skills_category ON skills(skill_category)"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
            
            self.connection.commit()
            self.logger.info("✅ Дополнительные индексы созданы")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Не удалось создать дополнительные индексы: {e}")

    def _prepare_vacancy_data(self, vacancy: Dict) -> tuple:
        """
        Подготавливает данные вакансии для вставки в БД.
        """
        # Базовые поля
        vacancy_id = self._generate_vacancy_id(vacancy)
        hh_id = vacancy.get('id', '')
        name = vacancy.get('name', '')
        name_cleaned = name.lower() if name else ''
        
        # Локация
        area_data = vacancy.get('area', {})
        area = area_data.get('name', '')
        area_id = area_data.get('id', 0)
        region = vacancy.get('region', '')
        
        # Зарплата
        salary_data = vacancy.get('salary', {})
        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        salary_currency = salary_data.get('currency', '')
        salary_avg_rub = self._calculate_avg_salary_rub(salary_data)
        has_salary = 1 if salary_avg_rub else 0
        
        # Опыт и график
        experience_data = vacancy.get('experience', {})
        experience = experience_data.get('name', '')
        
        schedule_data = vacancy.get('schedule', {})
        schedule = schedule_data.get('name', '')
        
        employment_data = vacancy.get('employment', {})
        employment = employment_data.get('name', '')
        
        # Работодатель
        employer_data = vacancy.get('employer', {})
        employer_name = employer_data.get('name', '')
        employer_id = employer_data.get('id', '')
        employer_trusted = 1 if employer_data.get('trusted') else 0
        
        # Промышленная классификация
        industry_segment = self._classify_industry_segment(vacancy)
        position_level = self._classify_position_level(vacancy)
        
        professional_roles = self._extract_professional_roles(vacancy)
        industrial_keywords = self._extract_industrial_keywords(vacancy)
        
        # Навыки
        key_skills = vacancy.get('key_skills', [])
        key_skills_json = json.dumps(key_skills, ensure_ascii=False) if key_skills else '[]'
        
        # Временные метки
        published_at = self._parse_datetime(vacancy.get('published_at'))
        created_at = self._parse_datetime(vacancy.get('created_at'))
        collected_at = self._parse_datetime(vacancy.get('collected_at'))
        
        # Метод сбора
        collection_method = vacancy.get('collection_method', 'industrial_client')
        collection_source = 'FINAL_MERGED_INDUSTRIAL_VACANCIES'
        
        # Сниппеты
        snippet_data = vacancy.get('snippet', {})
        snippet_requirement = snippet_data.get('requirement', '')
        snippet_responsibility = snippet_data.get('responsibility', '')
        
        # Промышленный флаг - ВСЕГДА 1 (данные уже промышленные)
        is_industrial = 1
        
        return (
            vacancy_id, hh_id, name, name_cleaned, area, area_id, region,
            salary_from, salary_to, salary_currency, salary_avg_rub,
            experience, schedule, employment, employer_name, employer_id,
            employer_trusted, industry_segment, position_level,
            professional_roles, industrial_keywords, key_skills_json,
            published_at, created_at, collected_at, collection_method,
            snippet_requirement, snippet_responsibility, has_salary, is_industrial
        )

    def _generate_vacancy_id(self, vacancy: Dict) -> int:
        """
        Генерирует числовой ID для вакансии.
        """
        hh_id = vacancy.get('id', '')
        if hh_id and hh_id.isdigit():
            return int(hh_id)
        else:
            # Генерируем хэш-based ID для строковых ID
            return int(hashlib.md5(hh_id.encode()).hexdigest()[:8], 16)

    def _calculate_avg_salary_rub(self, salary_data: Dict) -> Optional[int]:
        """
        Расчет средней зарплаты в рублях.
        """
        if not salary_data:
            return None
            
        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        currency = salary_data.get('currency', '').upper()
        
        # Конвертация в рубли
        exchange_rates = {
            'RUR': 1.0, 'RUB': 1.0,
            'USD': 95.0, 'EUR': 100.0,
            'KZT': 0.2, 'BYR': 30.0
        }
        
        rate = exchange_rates.get(currency, 1.0)
        
        # Расчет средней зарплаты
        if salary_from and salary_to:
            avg_salary = (salary_from + salary_to) / 2
        elif salary_from:
            avg_salary = salary_from * 1.2  # +20% для "от"
        elif salary_to:
            avg_salary = salary_to * 0.8    # -20% для "до"
        else:
            return None
            
        return int(avg_salary * rate)

    def _classify_industry_segment(self, vacancy: Dict) -> str:
        """
        Классификация отраслевого сегмента.
        Использует улучшенную классификацию из classification_config.py если доступна.
        """
        if USE_IMPORTED_CLASSIFIERS:
            name = vacancy.get('name', '')
            employer_name = vacancy.get('employer', {}).get('name', '')
            return classify_industry_segment(name, employer_name)
        
        # Fallback на старую логику если импорт не удался
        name = vacancy.get('name', '').lower()
        employer_name = vacancy.get('employer', {}).get('name', '').lower()
        
        # Ключевые слова для сегментов (упрощенная версия)
        segments_keywords = {
            'машиностроение': [
                'машиностроение', 'станкостроение', 'автомобилестроение',
                'авиастроение', 'судостроение', 'оборонпром', 'вагоностроение'
            ],
            'металлургия': [
                'металлург', 'сталевар', 'прокат', 'литейщ', 'металлообработк',
                'ковк', 'штампов', 'прессов'
            ],
            'химическая': [
                'химик', 'лаборант', 'технолог хими', 'нефтехим', 'полимер',
                'пластмасс', 'резинотехническ', 'лакокрасочн'
            ],
            'энергетика': [
                'энергетик', 'электрик', 'электромонтер', 'электромеханик',
                'релейщик', 'электроэнергетик', 'теплоэнергетик'
            ],
            'нефтегазовая': [
                'нефть', 'газ', 'буровик', 'нефтяник', 'газовик', 'нефтедобыча',
                'нефтепереработк', 'трубопровод'
            ],
            'горнодобывающая': [
                'горняк', 'взрывник', 'проходчик', 'маркшейдер', 'обогатитель',
                'шахт', 'рудник', 'карьер'
            ],
            'строительная': [
                'строитель', 'монтажник', 'каменщик', 'штукатур', 'маляр',
                'кровельщик', 'арматурщик', 'бетонщик'
            ],
            'приборостроение': [
                'кип', 'кипиа', 'приборист', 'асутп', 'автоматика', 'телемеханик',
                'радиоэлектрон', 'электронщик'
            ],
            'деревообрабатывающая': [
                'деревообработк', 'столяр', 'плотник', 'лесник', 'лесозаготовк',
                'мебельщ', 'паркетч'
            ],
            'пищевая': [
                'пищев', 'технолог пищев', 'аппаратчик пищев', 'оператор линии',
                'мукомол', 'кондитер', 'маслодел', 'сыродел'
            ]
        }
        
        for segment, keywords in segments_keywords.items():
            for keyword in keywords:
                if keyword in name or keyword in employer_name:
                    return segment
        
        return 'другое'

    def _classify_position_level(self, vacancy: Dict) -> str:
        """
        Классификация уровня позиции.
        Использует улучшенную классификацию из classification_config.py если доступна.
        """
        if USE_IMPORTED_CLASSIFIERS:
            name = vacancy.get('name', '')
            return classify_position_level(name)
        
        # Fallback на старую логику если импорт не удался
        name = vacancy.get('name', '').lower()
        
        levels_keywords = {
            'рабочий': [
                'рабочий', 'оператор', 'грузчик', 'слесарь', 'токарь', 'фрезеровщик',
                'сварщик', 'монтажник', 'электромонтер', 'наладчик'
            ],
            'специалист': [
                'специалист', 'технолог', 'мастер', 'бригадир', 'механик', 'электрик'
            ],
            'инженер': [
                'инженер', 'конструктор', 'проектировщик', 'техник'
            ],
            'руководитель': [
                'начальник', 'руководитель', 'директор', 'зам', 'заместитель',
                'управляющ', 'прораб', 'мастер участка'
            ],
            'высшее_руководство': [
                'генеральный', 'директор по развитию', 'технический директор',
                'главный инженер', 'главный технолог'
            ]
        }
        
        for level, keywords in levels_keywords.items():
            for keyword in keywords:
                if keyword in name:
                    return level
        
        return 'другое'

    def _extract_professional_roles(self, vacancy: Dict) -> str:
        """
        Извлекает профессиональные роли.
        """
        roles = vacancy.get('professional_roles', [])
        if roles:
            role_names = [role.get('name', '') for role in roles if role.get('name')]
            return ', '.join(role_names)
        return ''

    def _extract_industrial_keywords(self, vacancy: Dict) -> str:
        """
        Извлекает промышленные ключевые слова.
        """
        name = vacancy.get('name', '').lower()
        snippet = vacancy.get('snippet', {}).get('requirement', '').lower()
        
        industrial_keywords = set()
        
        # Список промышленных ключевых слов
        keywords_list = [
            'инженер', 'технолог', 'конструктор', 'механик', 'электрик',
            'сварщик', 'токарь', 'фрезеровщик', 'наладчик', 'оператор',
            'аппаратчик', 'машинист', 'монтажник', 'ремонтник', 'станочник'
        ]
        
        for keyword in keywords_list:
            if keyword in name or keyword in snippet:
                industrial_keywords.add(keyword)
        
        return ', '.join(industrial_keywords)

    def _parse_datetime(self, date_str: str) -> Optional[str]:
        """
        Парсит datetime строку.
        """
        if not date_str:
            return None
            
        try:
            # Пробуем разные форматы дат
            formats = [
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d"
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
                    
            return None
        except:
            return None

    def _insert_skills_batch(self, cursor, vacancy_id: int, skills: List[Dict]):
        """
        Вставляет навыки для вакансии.
        """
        if not skills:
            return
            
        for i, skill in enumerate(skills):
            try:
                skill_name = skill.get('name', '')
                if not skill_name:
                    continue
                    
                skill_category = self._categorize_skill(skill_name)
                frequency_rank = i + 1
                
                cursor.execute("""
                    INSERT INTO skills (vacancy_id, skill_name, skill_category, frequency_rank)
                    VALUES (?, ?, ?, ?)
                """, (vacancy_id, skill_name, skill_category, frequency_rank))
                
            except Exception as e:
                continue  # Пропускаем ошибки навыков

    def _categorize_skill(self, skill_name: str) -> str:
        """
        Категоризация навыков.
        """
        skill_lower = skill_name.lower()
        
        categories = {
            'технические': [
                'autocad', 'solidworks', 'компас', 'черчение', 'чтение чертежей',
                'техническое обслуживание', 'ремонт оборудования', 'наладка'
            ],
            'производственные': [
                'сварка', 'токарные работы', 'фрезерные работы', 'обработка металлов',
                'литейное производство', 'прокатное производство'
            ],
            'кипиа_асу_тп': [
                'кип', 'кипиа', 'асутп', 'телемеханика', 'автоматизация',
                'контрольно-измерительные приборы', 'средства автоматизации'
            ],
            'электротехнические': [
                'электромонтаж', 'электрооборудование', 'релейная защита',
                'электроснабжение', 'силовая электроника'
            ],
            'химические': [
                'химический анализ', 'лабораторные исследования', 'технологические процессы',
                'контроль качества', 'метрология'
            ],
            'управленческие': [
                'управление персоналом', 'планирование производства', 'контроль качества',
                'отчетность', 'ведение документации'
            ],
            'информационные': [
                '1с', 'ms office', 'excel', 'word', 'электронная почта',
                'делопроизводство', 'работа с базами данных'
            ],
            'безопасность': [
                'охрана труда', 'техника безопасности', 'промышленная безопасность',
                'пожарная безопасность', 'электробезопасность'
            ]
        }
        
        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in skill_lower:
                    return category
        
        return 'другие'

    def get_database_stats(self) -> Dict:
        """
        Возвращает статистику базы данных.
        """
        if not self.connection:
            return {}
            
        stats = {}
        cursor = self.connection.cursor()
        
        try:
            # Общая статистика
            cursor.execute("SELECT COUNT(*) as total FROM vacancies")
            stats['total_vacancies'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) as with_salary FROM vacancies WHERE has_salary = 1")
            stats['vacancies_with_salary'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT employer_name) as employers FROM vacancies")
            stats['unique_employers'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT region) as regions FROM vacancies")
            stats['unique_regions'] = cursor.fetchone()[0]
            
            # Статистика по сегментам
            cursor.execute("""
                SELECT industry_segment, COUNT(*) as count 
                FROM vacancies 
                GROUP BY industry_segment 
                ORDER BY count DESC
            """)
            stats['industry_segments'] = dict(cursor.fetchall())
            
            # Статистика по уровням
            cursor.execute("""
                SELECT position_level, COUNT(*) as count 
                FROM vacancies 
                GROUP BY position_level 
                ORDER BY count DESC
            """)
            stats['position_levels'] = dict(cursor.fetchall())
            
            # Статистика по навыкам
            cursor.execute("SELECT COUNT(DISTINCT skill_name) as skills FROM skills")
            stats['unique_skills'] = cursor.fetchone()[0]
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при получении статистики: {e}")
            
        return stats

    def close_connection(self):
        """Закрывает соединение с базой данных."""
        if self.connection:
            # Оптимизируем базу перед закрытием
            try:
                self.connection.execute("PRAGMA optimize")
                self.connection.execute("VACUUM")
            except:
                pass
                
            self.connection.close()
            self.logger.info("✅ Соединение с базой данных закрыто")


# Функция для быстрой загрузки данных
def load_industrial_data():
    """
    Быстрая загрузка данных из FINAL_MERGED_INDUSTRIAL_VACANCIES.json
    с упрощенной фильтрацией.
    """
    db_manager = IndustrialDatabaseManager()
    
    if db_manager.create_connection():
        # Путь к вашему файлу
        json_file = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
        
        if os.path.exists(json_file):
            inserted = db_manager.load_industrial_data_from_json(json_file)
            
            if inserted > 0:
                stats = db_manager.get_database_stats()
                print("\n📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
                print(f"   Всего вакансий: {stats.get('total_vacancies', 0):,}")
                print(f"   С зарплатой: {stats.get('vacancies_with_salary', 0):,}")
                print(f"   Работодателей: {stats.get('unique_employers', 0):,}")
                print(f"   Регионов: {stats.get('unique_regions', 0):,}")
                print(f"   Уникальных навыков: {stats.get('unique_skills', 0):,}")
                
                print("\n🏭 РАСПРЕДЕЛЕНИЕ ПО СЕГМЕНТАМ:")
                for segment, count in list(stats.get('industry_segments', {}).items())[:5]:
                    print(f"   {segment}: {count:,}")
            else:
                print("❌ Не удалось загрузить данные")
                    
        else:
            print(f"❌ Файл {json_file} не найден")
        
        db_manager.close_connection()
    else:
        print("❌ Не удалось подключиться к базе данных")


if __name__ == "__main__":
    load_industrial_data()