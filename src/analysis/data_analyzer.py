"""
ОБНОВЛЕННЫЙ АНАЛИЗ ДЛЯ 500K+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ
ИСПРАВЛЕННАЯ ВЕРСИЯ ДЛЯ SQLite (без MEDIAN)
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple, Optional, Any
import logging
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')

@dataclass
class AnalysisConfig:
    """Конфигурация анализа для больших данных."""
    min_vacancies_for_analysis: int = 1000
    sample_size_large: int = 100000
    chunk_size: int = 50000
    cache_results: bool = True

class IndustrialDataAnalyzer:
    """
    Анализатор для 500K+ промышленных вакансий.
    Оптимизирован для работы с SQLite.
    """
    
    def __init__(self, db_path: str = "industrial_vacancies.db"):
        self.db_path = db_path
        self.connection = None
        self.config = AnalysisConfig()
        self.logger = self._setup_logger()
        
        # Кэш для результатов
        self._cache = {}
        
        # Промышленные сегменты для анализа
        self.industrial_segments = [
            'машиностроение', 'металлургия', 'химическая', 'энергетика',
            'нефтегазовая', 'горнодобывающая', 'строительная', 
            'приборостроение', 'деревообрабатывающая', 'пищевая'
        ]
        
        # Уровни позиций
        self.position_levels = [
            'рабочий', 'специалист', 'инженер', 'руководитель', 'высшее_руководство'
        ]

    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования."""
        logger = logging.getLogger('IndustrialDataAnalyzer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger

    def connect_to_database(self) -> bool:
        """
        Подключение к базе данных с оптимизациями для анализа.
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            
            # Оптимизации для анализа
            self.connection.execute("PRAGMA cache_size = -128000")  # 128MB
            self.connection.execute("PRAGMA temp_store = MEMORY")
            
            self.connection.row_factory = sqlite3.Row
            self.logger.info(f"✅ Подключение к базе данных установлено")
            return True
            
        except sqlite3.Error as e:
            self.logger.error(f"❌ Ошибка подключения к базе данных: {e}")
            return False

    def get_basic_statistics(self) -> Dict[str, Any]:
        """
        Базовая статистика по промышленным вакансиям.
        SQLite-совместимая версия (без MEDIAN).
        """
        cache_key = "basic_statistics"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        stats = {}
        
        try:
            cursor = self.connection.cursor()
            
            # Основные метрики (без MEDIAN)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_vacancies,
                    COUNT(CASE WHEN has_salary = 1 THEN 1 END) as vacancies_with_salary,
                    COUNT(DISTINCT employer_name) as unique_employers,
                    COUNT(DISTINCT region) as unique_regions,
                    AVG(salary_avg_rub) as avg_salary,
                    MIN(salary_avg_rub) as min_salary,
                    MAX(salary_avg_rub) as max_salary
                FROM vacancies 
                WHERE is_industrial = 1
            """)
            
            row = cursor.fetchone()
            if row:
                stats.update(dict(row))
            
            # Вычисляем медиану отдельно (аппроксимация через процентили)
            cursor.execute("""
                SELECT salary_avg_rub
                FROM vacancies 
                WHERE is_industrial = 1 AND has_salary = 1
                ORDER BY salary_avg_rub
            """)
            salary_data = [row[0] for row in cursor.fetchall() if row[0] is not None]
            
            if salary_data:
                stats['median_salary'] = float(np.median(salary_data))
            else:
                stats['median_salary'] = 0
            
            # Статистика по датам
            cursor.execute("""
                SELECT 
                    MIN(published_at) as earliest_date,
                    MAX(published_at) as latest_date,
                    COUNT(DISTINCT DATE(published_at)) as unique_days
                FROM vacancies 
                WHERE is_industrial = 1
            """)
            
            date_stats = dict(cursor.fetchone())
            stats.update(date_stats)
            
            # Процентные показатели
            if stats.get('total_vacancies', 0) > 0:
                stats['salary_coverage_percent'] = (stats['vacancies_with_salary'] / stats['total_vacancies']) * 100
            else:
                stats['salary_coverage_percent'] = 0
            
            self.logger.info(f"📊 Базовая статистика собрана: {stats.get('total_vacancies', 0):,} вакансий")
            
            if self.config.cache_results:
                self._cache[cache_key] = stats
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка при сборе базовой статистики: {e}")
            
        return stats

    def analyze_industry_segments_distribution(self) -> Dict[str, Any]:
        """
        Детальный анализ распределения по отраслевым сегментам.
        SQLite-совместимая версия.
        """
        cache_key = "industry_segments"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            # Базовый запрос без MEDIAN
            query = """
                SELECT 
                    industry_segment,
                    COUNT(*) as vacancy_count,
                    COUNT(CASE WHEN has_salary = 1 THEN 1 END) as with_salary_count,
                    AVG(salary_avg_rub) as avg_salary,
                    COUNT(DISTINCT employer_name) as unique_employers,
                    COUNT(DISTINCT region) as unique_regions
                FROM vacancies 
                WHERE is_industrial = 1 AND industry_segment IS NOT NULL
                GROUP BY industry_segment
                ORDER BY vacancy_count DESC
            """
            
            df = pd.read_sql_query(query, self.connection)
            
            if not df.empty:
                total_vacancies = df['vacancy_count'].sum()
                df['percentage'] = (df['vacancy_count'] / total_vacancies) * 100
                df['salary_coverage_percent'] = (df['with_salary_count'] / df['vacancy_count']) * 100
                
                # Вычисляем медиану для каждого сегмента отдельно
                medians = {}
                for segment in df['industry_segment'].unique():
                    median_query = f"""
                        SELECT salary_avg_rub 
                        FROM vacancies 
                        WHERE is_industrial = 1 AND industry_segment = ? AND has_salary = 1
                        ORDER BY salary_avg_rub
                    """
                    cursor = self.connection.cursor()
                    cursor.execute(median_query, (segment,))
                    salaries = [row[0] for row in cursor.fetchall() if row[0] is not None]
                    medians[segment] = float(np.median(salaries)) if salaries else 0
                
                df['median_salary'] = df['industry_segment'].map(medians)
                
                analysis['segments'] = df.to_dict('records')
                analysis['total_segments'] = len(df)
                analysis['dominant_segment'] = df.iloc[0]['industry_segment']
                analysis['dominant_percentage'] = df.iloc[0]['percentage']
                
                # Топ-5 сегментов
                analysis['top_segments'] = df.head(5).to_dict('records')
                
                self.logger.info(f"🏭 Проанализировано {len(df)} отраслевых сегментов")
                
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа отраслевых сегментов: {e}")
            
        return analysis

    def analyze_position_levels_distribution(self) -> Dict[str, Any]:
        """
        Анализ распределения по уровням позиций.
        SQLite-совместимая версия.
        """
        cache_key = "position_levels"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            query = """
                SELECT 
                    position_level,
                    COUNT(*) as vacancy_count,
                    AVG(salary_avg_rub) as avg_salary,
                    COUNT(DISTINCT industry_segment) as segments_covered,
                    COUNT(DISTINCT region) as regions_covered
                FROM vacancies 
                WHERE is_industrial = 1 AND position_level IS NOT NULL
                GROUP BY position_level
                ORDER BY vacancy_count DESC
            """
            
            df = pd.read_sql_query(query, self.connection)
            
            if not df.empty:
                total_vacancies = df['vacancy_count'].sum()
                df['percentage'] = (df['vacancy_count'] / total_vacancies) * 100
                
                # Вычисляем медиану для каждого уровня
                medians = {}
                for level in df['position_level'].unique():
                    median_query = f"""
                        SELECT salary_avg_rub 
                        FROM vacancies 
                        WHERE is_industrial = 1 AND position_level = ? AND has_salary = 1
                        ORDER BY salary_avg_rub
                    """
                    cursor = self.connection.cursor()
                    cursor.execute(median_query, (level,))
                    salaries = [row[0] for row in cursor.fetchall() if row[0] is not None]
                    medians[level] = float(np.median(salaries)) if salaries else 0
                
                df['median_salary'] = df['position_level'].map(medians)
                
                analysis['levels'] = df.to_dict('records')
                analysis['most_demanded_level'] = df.iloc[0]['position_level']
                analysis['most_demanded_count'] = df.iloc[0]['vacancy_count']
                
                if not df.empty and 'avg_salary' in df.columns and df['avg_salary'].notna().any():
                    analysis['highest_paid_level'] = df.loc[df['avg_salary'].idxmax()]['position_level']
                    analysis['highest_salary'] = df['avg_salary'].max()
                else:
                    analysis['highest_paid_level'] = 'нет данных'
                    analysis['highest_salary'] = 0
                
                self.logger.info(f"📊 Проанализировано {len(df)} уровней позиций")
                
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа уровней позиций: {e}")
            
        return analysis

    def analyze_salary_comparison(self) -> Dict[str, Any]:
        """
        Детальное сравнение зарплат по сегментам и уровням.
        SQLite-совместимая версия.
        """
        cache_key = "salary_comparison"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            # Зарплаты по уровням позиций (без STDDEV и MEDIAN)
            query_levels = """
                SELECT 
                    position_level,
                    COUNT(*) as count,
                    AVG(salary_avg_rub) as mean_salary,
                    MIN(salary_avg_rub) as min_salary,
                    MAX(salary_avg_rub) as max_salary
                FROM vacancies 
                WHERE is_industrial = 1 AND has_salary = 1 AND position_level IS NOT NULL
                GROUP BY position_level
                HAVING count >= 10
                ORDER BY mean_salary DESC
            """
            
            df_levels = pd.read_sql_query(query_levels, self.connection)
            
            # Вычисляем медиану отдельно
            medians_levels = {}
            for level in df_levels['position_level'].unique():
                median_query = f"""
                    SELECT salary_avg_rub 
                    FROM vacancies 
                    WHERE is_industrial = 1 AND position_level = ? AND has_salary = 1
                    ORDER BY salary_avg_rub
                """
                cursor = self.connection.cursor()
                cursor.execute(median_query, (level,))
                salaries = [row[0] for row in cursor.fetchall() if row[0] is not None]
                medians_levels[level] = float(np.median(salaries)) if salaries else 0
            
            df_levels['median_salary'] = df_levels['position_level'].map(medians_levels)
            analysis['by_position_level'] = df_levels.to_dict('records')
            
            # Зарплаты по отраслевым сегментам
            query_segments = """
                SELECT 
                    industry_segment,
                    COUNT(*) as count,
                    AVG(salary_avg_rub) as mean_salary,
                    MIN(salary_avg_rub) as min_salary,
                    MAX(salary_avg_rub) as max_salary
                FROM vacancies 
                WHERE is_industrial = 1 AND has_salary = 1 AND industry_segment IS NOT NULL
                GROUP BY industry_segment
                HAVING count >= 10
                ORDER BY mean_salary DESC
            """
            
            df_segments = pd.read_sql_query(query_segments, self.connection)
            
            # Вычисляем медиану для сегментов
            medians_segments = {}
            for segment in df_segments['industry_segment'].unique():
                median_query = f"""
                    SELECT salary_avg_rub 
                    FROM vacancies 
                    WHERE is_industrial = 1 AND industry_segment = ? AND has_salary = 1
                    ORDER BY salary_avg_rub
                """
                cursor = self.connection.cursor()
                cursor.execute(median_query, (segment,))
                salaries = [row[0] for row in cursor.fetchall() if row[0] is not None]
                medians_segments[segment] = float(np.median(salaries)) if salaries else 0
            
            df_segments['median_salary'] = df_segments['industry_segment'].map(medians_segments)
            analysis['by_industry_segment'] = df_segments.to_dict('records')
            
            # Сравнение инженеров vs рабочих
            query_comparison = """
                SELECT 
                    position_level,
                    COUNT(*) as count,
                    AVG(salary_avg_rub) as avg_salary,
                    industry_segment
                FROM vacancies 
                WHERE is_industrial = 1 AND has_salary = 1 
                AND position_level IN ('инженер', 'рабочий')
                GROUP BY position_level, industry_segment
                HAVING count >= 5
            """
            
            df_comparison = pd.read_sql_query(query_comparison, self.connection)
            
            if not df_comparison.empty:
                engineer_data = df_comparison[df_comparison['position_level'] == 'инженер']
                worker_data = df_comparison[df_comparison['position_level'] == 'рабочий']
                
                engineer_avg = engineer_data['avg_salary'].mean() if not engineer_data.empty else 0
                worker_avg = worker_data['avg_salary'].mean() if not worker_data.empty else 0
                
                analysis['engineer_vs_worker'] = {
                    'engineer_total_count': engineer_data['count'].sum() if not engineer_data.empty else 0,
                    'engineer_avg_salary': engineer_avg,
                    'worker_total_count': worker_data['count'].sum() if not worker_data.empty else 0,
                    'worker_avg_salary': worker_avg,
                    'salary_ratio': engineer_avg / worker_avg if worker_avg > 0 else 0,
                    'segment_comparison': df_comparison.to_dict('records')
                }
            
            self.logger.info("💰 Анализ зарплат завершен")
            
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа зарплат: {e}")
            
        return analysis

    def analyze_dynamics(self, period: str = 'monthly') -> Dict[str, Any]:
        """
        Анализ динамики изменения спроса.
        SQLite-совместимая версия.
        """
        cache_key = f"dynamics_{period}"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            if period == 'monthly':
                period_format = "strftime('%Y-%m', published_at)"
                period_display = "period || '-01'"
            elif period == 'weekly':
                period_format = "strftime('%Y-%W', published_at)"
                period_display = "period || '-1'"
            else:  # daily
                period_format = "DATE(published_at)"
                period_display = "period"
            
            query = f"""
                SELECT 
                    {period_format} as period,
                    COUNT(*) as vacancy_count,
                    AVG(salary_avg_rub) as avg_salary,
                    COUNT(DISTINCT industry_segment) as segments_active,
                    COUNT(DISTINCT region) as regions_active
                FROM vacancies 
                WHERE is_industrial = 1 AND published_at IS NOT NULL
                GROUP BY period
                ORDER BY period
            """
            
            df = pd.read_sql_query(query, self.connection)
            
            if not df.empty:
                # Преобразуем периоды в строки для JSON сериализации
                df['period_date'] = df['period'].astype(str)
                
                df = df.sort_values('period')
                
                # Расчет темпов роста
                if len(df) > 1:
                    first_count = df['vacancy_count'].iloc[0]
                    last_count = df['vacancy_count'].iloc[-1]
                    growth_rate = ((last_count - first_count) / first_count) * 100 if first_count > 0 else 0
                    
                    # Скользящее среднее
                    df['moving_avg'] = df['vacancy_count'].rolling(window=3, min_periods=1).mean()
                    
                    # Конвертируем в словарь с сериализуемыми типами
                    analysis['dynamics'] = []
                    for _, row in df.iterrows():
                        analysis['dynamics'].append({
                            'period': str(row['period']),
                            'vacancy_count': int(row['vacancy_count']),
                            'avg_salary': float(row['avg_salary']) if pd.notna(row['avg_salary']) else 0,
                            'segments_active': int(row['segments_active']),
                            'regions_active': int(row['regions_active']),
                            'moving_avg': float(row['moving_avg']) if pd.notna(row['moving_avg']) else 0
                        })
                    
                    analysis['total_periods'] = len(df)
                    analysis['growth_rate'] = float(growth_rate)
                    analysis['peak_period'] = str(df.loc[df['vacancy_count'].idxmax()]['period'])
                    analysis['peak_count'] = int(df['vacancy_count'].max())
                    
                    # Тренд (линейная регрессия)
                    x = np.arange(len(df))
                    y = df['vacancy_count'].values
                    z = np.polyfit(x, y, 1)
                    analysis['trend_slope'] = float(z[0])  # Положительный = рост, отрицательный = спад
                    
                self.logger.info(f"📈 Проанализирована динамика за {len(df)} периодов")
                
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа динамики: {e}")
            
        return analysis

    def analyze_skills_distribution(self, top_n: int = 20) -> Dict[str, Any]:
        """
        Анализ наиболее востребованных навыков.
        Исправленная версия для правильной структуры таблицы skills.
        """
        cache_key = f"skills_{top_n}"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            # Проверяем существование таблицы skills
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='skills'")
            if not cursor.fetchone():
                self.logger.warning("⚠️ Таблица skills не существует")
                analysis['top_skills'] = []
                analysis['skill_categories'] = []
                analysis['stats'] = {}
                return analysis
            
            # Топ навыков (исправленный запрос)
            query_skills = """
                SELECT 
                    skill_name,
                    skill_category,
                    COUNT(*) as frequency,
                    COUNT(DISTINCT vacancy_id) as unique_vacancies,
                    COUNT(DISTINCT industry_segment) as segments_covered
                FROM skills s
                JOIN vacancies v ON s.vacancy_id = v.id
                WHERE v.is_industrial = 1
                GROUP BY skill_name, skill_category
                ORDER BY frequency DESC
                LIMIT ?
            """
            
            df_skills = pd.read_sql_query(query_skills, self.connection, params=(top_n,))
            
            # Навыки по категориям
            query_categories = """
                SELECT 
                    skill_category,
                    COUNT(*) as skill_count,
                    COUNT(DISTINCT skill_name) as unique_skills,
                    COUNT(DISTINCT vacancy_id) as vacancies_with_skills
                FROM skills s
                JOIN vacancies v ON s.vacancy_id = v.id
                WHERE v.is_industrial = 1
                GROUP BY skill_category
                ORDER BY skill_count DESC
            """
            
            df_categories = pd.read_sql_query(query_categories, self.connection)
            
            # Общая статистика навыков (исправленный запрос)
            query_stats = """
                SELECT 
                    COUNT(DISTINCT s.skill_name) as total_unique_skills,
                    COUNT(*) as total_skill_mentions,
                    AVG(skills_per_vacancy) as avg_skills_per_vacancy
                FROM (
                    SELECT 
                        vacancy_id,
                        COUNT(*) as skills_per_vacancy
                    FROM skills s
                    JOIN vacancies v ON s.vacancy_id = v.id
                    WHERE v.is_industrial = 1
                    GROUP BY vacancy_id
                ) vacancy_stats,
                skills s
                WHERE s.vacancy_id = vacancy_stats.vacancy_id
            """
            
            try:
                df_stats = pd.read_sql_query(query_stats, self.connection)
                analysis['stats'] = df_stats.iloc[0].to_dict() if not df_stats.empty else {}
            except:
                # Альтернативный запрос если основной не работает
                cursor.execute("SELECT COUNT(DISTINCT skill_name) as total_unique_skills FROM skills")
                unique_skills = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) as total_skill_mentions FROM skills")
                total_mentions = cursor.fetchone()[0]
                analysis['stats'] = {
                    'total_unique_skills': unique_skills,
                    'total_skill_mentions': total_mentions,
                    'avg_skills_per_vacancy': 0
                }
            
            analysis['top_skills'] = df_skills.to_dict('records')
            analysis['skill_categories'] = df_categories.to_dict('records')
            
            self.logger.info(f"🔧 Проанализировано {len(df_skills)} топ навыков")
            
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа навыков: {e}")
            analysis['top_skills'] = []
            analysis['skill_categories'] = []
            analysis['stats'] = {}
            
        return analysis

    def analyze_regional_distribution(self) -> Dict[str, Any]:
        """
        Анализ регионального распределения вакансий.
        SQLite-совместимая версия.
        """
        cache_key = "regional_distribution"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            query = """
                SELECT 
                    region,
                    COUNT(*) as vacancy_count,
                    COUNT(CASE WHEN has_salary = 1 THEN 1 END) as with_salary_count,
                    AVG(salary_avg_rub) as avg_salary,
                    COUNT(DISTINCT industry_segment) as segments_present,
                    COUNT(DISTINCT employer_name) as unique_employers
                FROM vacancies 
                WHERE is_industrial = 1 AND region IS NOT NULL
                GROUP BY region
                HAVING vacancy_count >= 10
                ORDER BY vacancy_count DESC
            """
            
            df = pd.read_sql_query(query, self.connection)
            
            if not df.empty:
                total_vacancies = df['vacancy_count'].sum()
                df['percentage'] = (df['vacancy_count'] / total_vacancies) * 100
                df['salary_coverage_percent'] = (df['with_salary_count'] / df['vacancy_count']) * 100
                
                analysis['regions'] = df.to_dict('records')
                analysis['top_regions'] = df.head(10).to_dict('records')
                analysis['total_regions'] = len(df)
                analysis['dominant_region'] = df.iloc[0]['region']
                analysis['dominant_region_percentage'] = df.iloc[0]['percentage']
                
                if not df.empty and 'avg_salary' in df.columns and df['avg_salary'].notna().any():
                    analysis['highest_paid_region'] = df.loc[df['avg_salary'].idxmax()]['region']
                    analysis['highest_region_salary'] = float(df['avg_salary'].max())
                else:
                    analysis['highest_paid_region'] = 'нет данных'
                    analysis['highest_region_salary'] = 0
                
                # Регионы с наибольшим разнообразием сегментов
                analysis['most_diverse_regions'] = df.nlargest(5, 'segments_present').to_dict('records')
                
                self.logger.info(f"🌍 Проанализировано {len(df)} регионов")
                
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа регионального распределения: {e}")
            
        return analysis

    def analyze_industrial_depth(self) -> Dict[str, Any]:
        """
        Глубокий анализ промышленных характеристик.
        SQLite-совместимая версия.
        """
        cache_key = "industrial_depth"
        if self.config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]
            
        analysis = {}
        
        try:
            # Анализ по комбинациям сегмент-уровень
            query_combinations = """
                SELECT 
                    industry_segment,
                    position_level,
                    COUNT(*) as vacancy_count,
                    AVG(salary_avg_rub) as avg_salary,
                    COUNT(DISTINCT region) as regions_covered
                FROM vacancies 
                WHERE is_industrial = 1 
                AND industry_segment IS NOT NULL 
                AND position_level IS NOT NULL
                GROUP BY industry_segment, position_level
                HAVING vacancy_count >= 5
                ORDER BY industry_segment, position_level
            """
            
            df_combinations = pd.read_sql_query(query_combinations, self.connection)
            analysis['segment_level_combinations'] = df_combinations.to_dict('records')
            
            # Наиболее востребованные комбинации
            analysis['top_combinations'] = df_combinations.nlargest(10, 'vacancy_count').to_dict('records')
            
            # Анализ промышленных ключевых слов
            query_keywords = """
                SELECT 
                    industrial_keywords,
                    COUNT(*) as frequency
                FROM vacancies 
                WHERE is_industrial = 1 AND industrial_keywords IS NOT NULL AND industrial_keywords != ''
                GROUP BY industrial_keywords
                ORDER BY frequency DESC
                LIMIT 20
            """
            
            df_keywords = pd.read_sql_query(query_keywords, self.connection)
            analysis['industrial_keywords'] = df_keywords.to_dict('records')
            
            self.logger.info("🏭 Глубокий промышленный анализ завершен")
            
            if self.config.cache_results:
                self._cache[cache_key] = analysis
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка глубинного анализа: {e}")
            
        return analysis

    def generate_comprehensive_report(self) -> Dict[str, Any]:
        """
        Генерация комплексного отчета по всем аспектам.
        JSON-сериализуемая версия.
        """
        self.logger.info("📋 Генерация комплексного отчета...")
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'basic_statistics': self._make_json_serializable(self.get_basic_statistics()),
            'industry_segments': self._make_json_serializable(self.analyze_industry_segments_distribution()),
            'position_levels': self._make_json_serializable(self.analyze_position_levels_distribution()),
            'salary_comparison': self._make_json_serializable(self.analyze_salary_comparison()),
            'dynamics': self._make_json_serializable(self.analyze_dynamics('monthly')),
            'skills': self._make_json_serializable(self.analyze_skills_distribution(25)),
            'regional': self._make_json_serializable(self.analyze_regional_distribution()),
            'industrial_depth': self._make_json_serializable(self.analyze_industrial_depth())
        }
        
        # Ключевые выводы
        report['key_findings'] = self._extract_key_findings(report)
        
        self.logger.info("✅ Комплексный отчет сгенерирован")
        return report

    def _make_json_serializable(self, obj: Any) -> Any:
        """
        Преобразует объект в JSON-сериализуемый формат.
        """
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        else:
            return obj

    def _extract_key_findings(self, report: Dict) -> List[str]:
        """
        Извлекает ключевые выводы из отчета.
        """
        findings = []
        
        try:
            basic_stats = report['basic_statistics']
            segments = report['industry_segments']
            levels = report['position_levels']
            salaries = report['salary_comparison']
            
            # Основные метрики
            total_vacancies = basic_stats.get('total_vacancies', 0)
            if total_vacancies >= 1000:
                findings.append(f"Собран репрезентативный объем данных: {total_vacancies:,} промышленных вакансий")
            
            # Доминирующий сегмент
            if segments.get('dominant_segment'):
                findings.append(f"Наибольшее количество вакансий в сегменте: {segments['dominant_segment']} ({segments.get('dominant_percentage', 0):.1f}%)")
            
            # Наиболее востребованный уровень
            if levels.get('most_demanded_level'):
                findings.append(f"Наиболее востребованы специалисты уровня: {levels['most_demanded_level']}")
            
            # Сравнение зарплат
            if salaries.get('engineer_vs_worker'):
                ratio = salaries['engineer_vs_worker'].get('salary_ratio', 0)
                if ratio > 1:
                    findings.append(f"Инженеры получают в {ratio:.1f} раз больше рабочих")
            
            # Динамика
            dynamics = report['dynamics']
            if dynamics.get('growth_rate'):
                growth = dynamics['growth_rate']
                if growth > 0:
                    findings.append(f"Положительная динамика спроса: +{growth:.1f}% за период")
                elif growth < 0:
                    findings.append(f"Отрицательная динамика спроса: {growth:.1f}% за период")
            
            # Региональное распределение
            regional = report['regional']
            if regional.get('dominant_region'):
                findings.append(f"Лидирующий регион: {regional['dominant_region']} ({regional.get('dominant_region_percentage', 0):.1f}% вакансий)")
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка извлечения выводов: {e}")
            
        return findings

    def clear_cache(self):
        """Очищает кэш результатов."""
        self._cache.clear()
        self.logger.info("🧹 Кэш результатов очищен")

    def close_connection(self):
        """Закрывает соединение с базой данных."""
        if self.connection:
            self.connection.close()
            self.logger.info("✅ Соединение с базой данных закрыто")


# Функция для быстрого запуска анализа
def run_industrial_analysis():
    """
    Быстрый запуск полного анализа промышленных вакансий.
    """
    analyzer = IndustrialDataAnalyzer()
    
    if analyzer.connect_to_database():
        print("🚀 ЗАПУСК АНАЛИЗА ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
        print("=" * 60)
        
        # Базовая статистика
        stats = analyzer.get_basic_statistics()
        print(f"📊 Всего вакансий: {stats.get('total_vacancies', 0):,}")
        print(f"💰 Со зарплатой: {stats.get('vacancies_with_salary', 0):,} ({stats.get('salary_coverage_percent', 0):.1f}%)")
        print(f"🏢 Работодателей: {stats.get('unique_employers', 0):,}")
        print(f"🌍 Регионов: {stats.get('unique_regions', 0):,}")
        print(f"💸 Средняя зарплата: {stats.get('avg_salary', 0):,.0f} руб")
        print(f"📈 Медианная зарплата: {stats.get('median_salary', 0):,.0f} руб")
        
        # Генерация полного отчета
        report = analyzer.generate_comprehensive_report()
        
        # Ключевые выводы
        print("\n🎯 КЛЮЧЕВЫЕ ВЫВОДЫ:")
        findings = report.get('key_findings', [])
        if findings:
            for i, finding in enumerate(findings[:5], 1):
                print(f"  {i}. {finding}")
        else:
            print("  ℹ️  Ключевые выводы не сгенерированы")
        
        analyzer.close_connection()
        
        return report
    else:
        print("❌ Не удалось подключиться к базе данных")
        return None


if __name__ == "__main__":
    report = run_industrial_analysis()
    if report:
        print(f"\n✅ Анализ завершен. Отчет сгенерирован: {report['generated_at']}")