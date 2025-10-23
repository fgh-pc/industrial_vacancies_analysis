"""
Модуль для анализа данных о вакансиях в промышленной отрасли.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from typing import Dict, List, Tuple, Optional

class DataAnalyzer:
    """
    Анализатор данных о вакансиях промышленной отрасли.
    """
    
    def __init__(self, db_path: str = "industrial_vacancies.db"):
        """
        Инициализация анализатора с путем к базе данных.
        
        Args:
            db_path (str): Путь к файлу SQLite базы данных
        """
        self.db_path = db_path
        self.connection = None
        self.df_vacancies = None
        self.df_skills = None
        
    def connect_to_database(self) -> bool:
        """
        Подключение к базе данных.
        
        Returns:
            bool: True если подключение успешно
        """
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"[V] Подключение к базе данных {self.db_path} установлено")
            return True
        except sqlite3.Error as e:
            print(f"[X] Ошибка подключения к базе данных: {e}")
            return False
            
    def load_data_to_dataframes(self):
        """
        Загрузка данных из SQLite в pandas DataFrame.
        """
        if not self.connection:
            print("[X] Нет подключения к базе данных")
            return
            
        try:
            # Загружаем вакансии
            query_vacancies = """
            SELECT 
                id, name, area, salary_from, salary_to, salary_currency,
                experience, schedule, employment, published_at, employer_name,
                snippet_requirement, snippet_responsibility
            FROM vacancies
            """
            self.df_vacancies = pd.read_sql_query(query_vacancies, self.connection)
            
            # Загружаем навыки
            query_skills = """
            SELECT vacancy_id, skill_name 
            FROM skills
            """
            self.df_skills = pd.read_sql_query(query_skills, self.connection)
            
            print(f"[V] Загружено {len(self.df_vacancies)} вакансий и {len(self.df_skills)} навыков")
            
        except Exception as e:
            print(f"[X] Ошибка загрузки данных: {e}")
            
    def preprocess_data(self):
        """
        Предварительная обработка данных: очистка и преобразование.
        """
        if self.df_vacancies is None:
            print("[X] Данные не загружены")
            return
            
        print(" Начинаем предобработку данных...")
        
        # 1. Преобразование даты публикации
        self.df_vacancies['published_at'] = pd.to_datetime(
            self.df_vacancies['published_at'], 
            errors='coerce',
            utc=True
        )
        
        # 2. Очистка и классификация названий вакансий
        self.df_vacancies['name_cleaned'] = self.df_vacancies['name'].str.lower()
        
        # 3. Расчет средней зарплаты
        self.df_vacancies['salary_avg'] = self.df_vacancies.apply(
            self._calculate_average_salary, axis=1
        )
        
        # 4. Конвертация валют (упрощенно - только рубли)
        self.df_vacancies['salary_avg_rub'] = self.df_vacancies.apply(
            self._convert_to_rubles, axis=1
        )
        
        # 5. Классификация уровня позиции
        self.df_vacancies['position_level'] = self.df_vacancies['name'].apply(
            self._classify_position_level
        )
        
        # 6. Классификация отраслевого сегмента
        self.df_vacancies['industry_segment'] = self.df_vacancies['name'].apply(
            self._classify_industry_segment
        )
        
        print("[V] Предобработка данных завершена")
        
    def _calculate_average_salary(self, row) -> Optional[float]:
        """
        Расчет средней зарплаты из вилки 'от' и 'до'.
        
        Args:
            row: Строка DataFrame с полями salary_from и salary_to
            
        Returns:
            Optional[float]: Средняя зарплата или None
        """
        try:
            salary_from = row['salary_from']
            salary_to = row['salary_to']
            
            if pd.notna(salary_from) and pd.notna(salary_to):
                return (salary_from + salary_to) / 2
            elif pd.notna(salary_from):
                return salary_from * 1.2  
            elif pd.notna(salary_to):
                return salary_to * 0.8    
            else:
                return None
                
        except (TypeError, ValueError):
            return None
            
    def _convert_to_rubles(self, row) -> Optional[float]:
        """
        Конвертация зарплаты в рубли (упрощенная версия).
        
        Args:
            row: Строка DataFrame с полями salary_avg и salary_currency
            
        Returns:
            Optional[float]: Зарплата в рублях или None
        """
        salary = row['salary_avg']
        currency = row['salary_currency']
        
        if pd.isna(salary):
            return None
            
        # Упрощенные курсы валют
        exchange_rates = {
            'RUR': 1.0,
            'RUB': 1.0,
            'USD': 95.0,
            'EUR': 100.0,
            'KZT': 0.2
        }
        
        rate = exchange_rates.get(currency, 1.0)
        return salary * rate
        
    def _classify_position_level(self, job_title: str) -> str:
        """
        Классификация уровня позиции по названию вакансии.
        
        Args:
            job_title (str): Название вакансии
            
        Returns:
            str: Уровень позиции
        """
        if not isinstance(job_title, str):
            return 'не определено'
            
        title_lower = job_title.lower()
        
        # Ключевые слова для классификации уровней
        keywords = {
            'рабочий': ['рабочий', 'оператор', 'грузчик', 'слесарь', 'токарь', 'фрезеровщик'],
            'специалист': ['специалист', 'менеджер', 'технолог', 'мастер', 'бригадир'],
            'инженер': ['инженер', 'конструктор', 'проектировщик'],
            'руководитель': ['начальник', 'руководитель', 'директор', 'зам', 'заместитель'],
            'высшее_руководство': ['генеральный', 'директор по развитию', 'технический директор']
        }
        
        for level, level_keywords in keywords.items():
            for keyword in level_keywords:
                if keyword in title_lower:
                    return level
                    
        return 'другое'
        
    def _classify_industry_segment(self, job_title: str) -> str:
        """
        Классификация отраслевого сегмента по названию вакансии.
        
        Args:
            job_title (str): Название вакансии
            
        Returns:
            str: Отраслевой сегмент
        """
        if not isinstance(job_title, str):
            return 'другое'
            
        title_lower = job_title.lower()
        
        # Ключевые слова для классификации сегментов промышленности
        segments = {
            'машиностроение': [
                'машиностроение', 'станкостроение', 'автомобилестроение',
                'авиастроение', 'судостроение', 'оборонпром'
            ],
            'металлургия': [
                'металлург', 'сталевар', 'прокат', 'литейщ', 'металлообработк'
            ],
            'химическая': [
                'химик', 'лаборант', 'технолог хими', 'нефтехим', 'полимер'
            ],
            'энергетика': [
                'энергетик', 'электрик', 'электромонтер', 'энерго', 'тепло'
            ],
            'нефтегазовая': [
                'нефть', 'газ', 'буровик', 'нефтяник', 'газовик', 'нефтедобыча'
            ],
            'строительная': [
                'строитель', 'монтажник', 'отделочник', 'каменщик', 'штукатур'
            ]
        }
        
        for segment, segment_keywords in segments.items():
            for keyword in segment_keywords:
                if keyword in title_lower:
                    return segment
                    
        return 'другое'
    
    def get_basic_statistics(self) -> Dict:
        """
        Получение базовой статистики по данным.
        
        Returns:
            Dict: Словарь со статистикой
        """
        if self.df_vacancies is None:
            return {}
            
        stats = {}
        
        # Общая статистика
        stats['total_vacancies'] = len(self.df_vacancies)
        stats['vacancies_with_salary'] = self.df_vacancies['salary_avg_rub'].notna().sum()
        stats['unique_employers'] = self.df_vacancies['employer_name'].nunique()
        stats['unique_regions'] = self.df_vacancies['area'].nunique()
        
        # Статистика по зарплатам
        salary_data = self.df_vacancies['salary_avg_rub'].dropna()
        if len(salary_data) > 0:
            stats['avg_salary'] = salary_data.mean()
            stats['median_salary'] = salary_data.median()
            stats['min_salary'] = salary_data.min()
            stats['max_salary'] = salary_data.max()
            
        # Статистика по датам
        date_data = self.df_vacancies['published_at'].dropna()
        if len(date_data) > 0:
            stats['date_range'] = {
                'start': date_data.min(),
                'end': date_data.max()
            }
            
        return stats
        
    def analyze_industry_segments(self) -> Dict:
        """
        Анализ распределения вакансий по отраслевым сегментам.
        
        Returns:
            Dict: Статистика по сегментам
        """
        if self.df_vacancies is None:
            return {}
            
        segment_stats = self.df_vacancies['industry_segment'].value_counts()
        
        result = {
            'distribution': segment_stats.to_dict(),
            'total_by_segment': len(self.df_vacancies),
            'top_segments': segment_stats.head(5).to_dict()
        }
        
        # Доля каждого сегмента в процентах
        total = len(self.df_vacancies)
        percentages = {segment: (count / total) * 100 
                      for segment, count in segment_stats.items()}
        result['percentages'] = percentages
        
        return result
        
    def analyze_position_levels(self) -> Dict:
        """
        Анализ распределения вакансий по уровням позиций.
        
        Returns:
            Dict: Статистика по уровням
        """
        if self.df_vacancies is None:
            return {}
            
        level_stats = self.df_vacancies['position_level'].value_counts()
        
        result = {
            'distribution': level_stats.to_dict(),
            'most_demanded_level': level_stats.index[0] if len(level_stats) > 0 else None,
            'most_demanded_count': level_stats.iloc[0] if len(level_stats) > 0 else 0
        }
        
        return result
        
    def analyze_salaries_comparison(self) -> Dict:
        """
        Сравнение зарплат по уровням позиций и сегментам.
        
        Returns:
            Dict: Сравнительная статистика зарплат
        """
        if self.df_vacancies is None:
            return {}
            
        result = {}
        
        # Зарплаты по уровням позиций
        salary_by_level = self.df_vacancies.groupby('position_level')['salary_avg_rub'].agg([
            'count', 'mean', 'median', 'std'
        ]).round(2)
        result['by_position_level'] = salary_by_level.to_dict('index')
        
        # Зарплаты по отраслевым сегментам
        salary_by_segment = self.df_vacancies.groupby('industry_segment')['salary_avg_rub'].agg([
            'count', 'mean', 'median', 'std'
        ]).round(2)
        result['by_industry_segment'] = salary_by_segment.to_dict('index')
        
        # Сравнение инженеров vs рабочих
        engineer_salaries = self.df_vacancies[
            self.df_vacancies['position_level'] == 'инженер'
        ]['salary_avg_rub'].dropna()
        
        worker_salaries = self.df_vacancies[
            self.df_vacancies['position_level'] == 'рабочий'
        ]['salary_avg_rub'].dropna()
        
        result['engineer_vs_worker'] = {
            'engineer_count': len(engineer_salaries),
            'engineer_avg_salary': engineer_salaries.mean() if len(engineer_salaries) > 0 else 0,
            'worker_count': len(worker_salaries),
            'worker_avg_salary': worker_salaries.mean() if len(worker_salaries) > 0 else 0,
            'salary_ratio': (engineer_salaries.mean() / worker_salaries.mean() 
                           if len(worker_salaries) > 0 and worker_salaries.mean() > 0 else 0)
        }
        
        return result
        
    def analyze_dynamics(self) -> Dict:
        """
        Анализ динамики изменения спроса на вакансии.
        
        Returns:
            Dict: Динамика по месяцам/неделям
        """
        if self.df_vacancies is None or self.df_vacancies['published_at'].isna().all():
            return {}
            
        # Группировка по месяцам
        self.df_vacancies['year_month'] = self.df_vacancies['published_at'].dt.to_period('M')
        monthly_dynamics = self.df_vacancies.groupby('year_month').size()
        
        # Группировка по неделям
        self.df_vacancies['year_week'] = self.df_vacancies['published_at'].dt.to_period('W')
        weekly_dynamics = self.df_vacancies.groupby('year_week').size()
        
        result = {
            'monthly': monthly_dynamics.to_dict(),
            'weekly': weekly_dynamics.to_dict(),
            'total_months': len(monthly_dynamics),
            'growth_rate': self._calculate_growth_rate(monthly_dynamics)
        }
        
        return result
        
    def _calculate_growth_rate(self, monthly_series) -> float:
        """
        Расчет темпа роста количества вакансий.
        
        Args:
            monthly_series: Series с количеством вакансий по месяцам
            
        Returns:
            float: Темп роста в процентах
        """
        if len(monthly_series) < 2:
            return 0
            
        values = list(monthly_series.values)
        first_month = values[0]
        last_month = values[-1]
        
        if first_month == 0:
            return 0
            
        return ((last_month - first_month) / first_month) * 100
        
    def analyze_skills(self) -> Dict:
        """
        Анализ наиболее востребованных навыков.
        
        Returns:
            Dict: Статистика по навыкам
        """
        if self.df_skills is None:
            return {}
            
        skill_stats = self.df_skills['skill_name'].value_counts()
        
        result = {
            'top_skills': skill_stats.head(20).to_dict(),
            'total_unique_skills': len(skill_stats),
            'avg_skills_per_vacancy': len(self.df_skills) / len(self.df_vacancies) if len(self.df_vacancies) > 0 else 0
        }
        
        return result
        
    def close_connection(self):
        """Закрытие соединения с базой данных."""
        if self.connection:
            self.connection.close()
            print("[V] Соединение с базой данных закрыто")


# Пример использования
if __name__ == "__main__":
    # Создаем анализатор
    analyzer = DataAnalyzer()
    
    # Подключаемся к базе
    if analyzer.connect_to_database():
        # Загружаем данные
        analyzer.load_data_to_dataframes()
        
        # Обрабатываем данные
        analyzer.preprocess_data()
        
        # Получаем статистику
        stats = analyzer.get_basic_statistics()
        print(" Базовая статистика:")
        
        russian_titles = {
            'total_vacancies': 'Всего вакансий',
            'vacancies_with_salary': 'Вакансий с указанной зарплатой',
            'unique_employers': 'Уникальных работодателей', 
            'unique_regions': 'Уникальных регионов',
            'avg_salary': 'Средняя зарплата',
            'median_salary': 'Медианная зарплата',
            'min_salary': 'Минимальная зарплата',
            'max_salary': 'Максимальная зарплата',
            'date_range': 'Диапазон дат публикации'
        }
        
        for key, value in stats.items():
            if key in russian_titles:
                if key == 'date_range' and isinstance(value, dict):
                    start = value['start'].strftime('%Y-%m-%d') if hasattr(value['start'], 'strftime') else value['start']
                    end = value['end'].strftime('%Y-%m-%d') if hasattr(value['end'], 'strftime') else value['end']
                    print(f"   {russian_titles[key]}: с {start} по {end}")
                elif key in ['avg_salary', 'median_salary', 'min_salary', 'max_salary']:
                    print(f"   {russian_titles[key]}: {value:,.0f} руб.")
                else:
                    print(f"   {russian_titles[key]}: {value}")
            else:
                print(f"   {key}: {value}")
            
        # Анализ сегментов
        segments = analyzer.analyze_industry_segments()
        
        segment_translations = {
            'другое': 'Другие отрасли',
            'энергетика': 'Энергетика',
            'нефтегазовая': 'Нефтегазовая промышленность', 
            'строительная': 'Строительная промышленность',
            'химическая': 'Химическая промышленность',
            'машиностроение': 'Машиностроение',
            'металлургия': 'Металлургия'
        }
        
        print(f"\n Распределение по отраслевым сегментам:")
        for segment, count in segments.get('top_segments', {}).items():
            percentage = segments.get('percentages', {}).get(segment, 0)
            segment_name = segment_translations.get(segment, segment)
            print(f"   {segment_name}: {count} вакансий ({percentage:.1f}%)")
            
        analyzer.close_connection()