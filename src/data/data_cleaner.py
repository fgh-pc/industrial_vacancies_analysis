"""
Модуль для очистки и обработки данных вакансий.
Содержит функции для обработки пропусков, аномалий и преобразования данных.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import json

class DataCleaner:
    """
    Класс для очистки и предобработки данных о вакансиях.
    """
    
    def __init__(self):
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Настройка логирования."""
        logger = logging.getLogger('DataCleaner')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
        
    def load_raw_data(self, file_path: str) -> List[Dict]:
        """
        Загрузка сырых данных из JSON файла.
        
        Args:
            file_path: Путь к JSON файлу
            
        Returns:
            List[Dict]: Список вакансий
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            self.logger.info(f"Загружено {len(data)} вакансий из {file_path}")
            return data
            
        except Exception as e:
            self.logger.error(f"Ошибка загрузки данных из {file_path}: {e}")
            return []
            
    def convert_to_dataframe(self, vacancies: List[Dict]) -> pd.DataFrame:
        """
        Конвертация списка вакансий в pandas DataFrame.
        
        Args:
            vacancies: Список вакансий
            
        Returns:
            pd.DataFrame: DataFrame с вакансиями
        """
        if not vacancies:
            return pd.DataFrame()
            
        # Создаем DataFrame из списка словарей
        df = pd.json_normalize(vacancies)
        self.logger.info(f"Создан DataFrame с {len(df)} строками и {len(df.columns)} столбцами")
        
        return df
        
    def clean_vacancies_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Основная функция очистки данных вакансий.
        
        Args:
            df: DataFrame с сырыми данными
            
        Returns:
            pd.DataFrame: Очищенный DataFrame
        """
        if df.empty:
            return df
            
        self.logger.info("Начинаем очистку данных...")
        
        # Создаем копию для работы
        cleaned_df = df.copy()
        
        # 1. Обработка дубликатов
        cleaned_df = self._remove_duplicates(cleaned_df)
        
        # 2. Обработка пропущенных значений
        cleaned_df = self._handle_missing_values(cleaned_df)
        
        # 3. Стандартизация текстовых полей
        cleaned_df = self._standardize_text_fields(cleaned_df)
        
        # 4. Обработка зарплат
        cleaned_df = self._clean_salary_data(cleaned_df)
        
        # 5. Обработка дат
        cleaned_df = self._clean_date_fields(cleaned_df)
        
        # 6. Обработка категориальных полей
        cleaned_df = self._clean_categorical_fields(cleaned_df)
        
        # 7. Обработка навыков
        cleaned_df = self._extract_and_clean_skills(cleaned_df)
        
        # 8. Создание новых признаков
        cleaned_df = self._create_new_features(cleaned_df)
        
        # 9. Удаление ненужных столбцов
        cleaned_df = self._remove_unnecessary_columns(cleaned_df)
        
        self.logger.info("Очистка данных завершена")
        return cleaned_df
        
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Удаление дубликатов вакансий."""
        initial_count = len(df)
        
        # Удаляем дубликаты по ID
        df_cleaned = df.drop_duplicates(subset=['id'], keep='first')
        
        removed_count = initial_count - len(df_cleaned)
        if removed_count > 0:
            self.logger.info(f"Удалено дубликатов: {removed_count}")
            
        return df_cleaned
        
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обработка пропущенных значений."""
        missing_report = {}
        
        for column in df.columns:
            missing_count = df[column].isna().sum()
            if missing_count > 0:
                missing_percentage = (missing_count / len(df)) * 100
                missing_report[column] = {
                    'count': missing_count,
                    'percentage': missing_percentage
                }
                
                # Специфичная обработка для разных типов столбцов
                if column in ['salary.from', 'salary.to', 'salary.currency']:
                    # Для зарплаты заполняем специальным значением
                    if column == 'salary.currency':
                        df[column] = df[column].fillna('UNKNOWN')
                    else:
                        df[column] = df[column].fillna(-1)  # -1 для обозначения отсутствия зарплаты
                        
                elif column in ['name', 'area.name', 'employer.name']:
                    # Для текстовых полей заполняем 'Не указано'
                    df[column] = df[column].fillna('Не указано')
                    
                elif 'experience' in column:
                    df[column] = df[column].fillna('Не указан')
                    
                elif 'schedule' in column or 'employment' in column:
                    df[column] = df[column].fillna('Не указано')
                    
        if missing_report:
            self.logger.info("Отчет о пропущенных значениях:")
            for col, info in missing_report.items():
                self.logger.info(f"  {col}: {info['count']} ({info['percentage']:.1f}%)")
                
        return df
        
    def _standardize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Стандартизация текстовых полей."""
        text_columns = ['name', 'area.name', 'employer.name', 
                       'snippet.requirement', 'snippet.responsibility']
        
        for column in text_columns:
            if column in df.columns:
                # Приводим к нижнему регистру и удаляем лишние пробелы
                df[column] = df[column].astype(str).str.lower().str.strip()
                
        return df
        
    def _clean_salary_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очистка и стандартизация данных о зарплате."""
        salary_columns = ['salary.from', 'salary.to', 'salary.currency', 'salary.gross']
        
        # Проверяем наличие столбцов с зарплатой
        existing_salary_cols = [col for col in salary_columns if col in df.columns]
        
        if not existing_salary_cols:
            self.logger.warning("Столбцы с зарплатой не найдены")
            return df
            
        # Создаем новые столбцы для очищенной зарплаты
        df['salary_from_cleaned'] = df['salary.from']
        df['salary_to_cleaned'] = df['salary.to']
        df['salary_currency_cleaned'] = df['salary.currency']
        
        # Конвертируем валюту в рубли (упрощенный подход)
        exchange_rates = {
            'RUR': 1.0, 'RUB': 1.0,
            'USD': 95.0, 'EUR': 100.0,
            'KZT': 0.2, 'BYR': 30.0,
            'UAH': 2.5, 'AZN': 55.0
        }
        
        def convert_salary(row):
            """Конвертация зарплата в рубли."""
            currency = row['salary_currency_cleaned']
            rate = exchange_rates.get(currency, 1.0)
            
            salary_from = row['salary_from_cleaned']
            salary_to = row['salary_to_cleaned']
            
            if pd.notna(salary_from) and salary_from != -1:
                salary_from_rub = salary_from * rate
            else:
                salary_from_rub = np.nan
                
            if pd.notna(salary_to) and salary_to != -1:
                salary_to_rub = salary_to * rate
            else:
                salary_to_rub = np.nan
                
            return salary_from_rub, salary_to_rub
            
        # Применяем конвертацию
        salary_converted = df.apply(convert_salary, axis=1, result_type='expand')
        df['salary_from_rub'] = salary_converted[0]
        df['salary_to_rub'] = salary_converted[1]
        
        # Рассчитываем среднюю зарплату
        def calculate_avg_salary(row):
            """Расчет средней зарплаты."""
            salary_from = row['salary_from_rub']
            salary_to = row['salary_to_rub']
            
            if pd.notna(salary_from) and pd.notna(salary_to):
                return (salary_from + salary_to) / 2
            elif pd.notna(salary_from):
                return salary_from * 1.2  # Предполагаем рост
            elif pd.notna(salary_to):
                return salary_to * 0.8    # Предполагаем снижение
            else:
                return np.nan
                
        df['salary_avg_rub'] = df.apply(calculate_avg_salary, axis=1)
        
        # Флаг наличия зарплаты
        df['has_salary'] = df['salary_avg_rub'].notna()
        
        self.logger.info(f"Зарплаты обработаны. Вакансий с зарплатой: {df['has_salary'].sum()}")
        
        return df
        
    def _clean_date_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очистка и преобразование полей с датами."""
        if 'published_at' in df.columns:
            # Преобразуем в datetime
            df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
            
            # Извлекаем компоненты даты
            df['published_year'] = df['published_at'].dt.year
            df['published_month'] = df['published_at'].dt.month
            df['published_week'] = df['published_at'].dt.isocalendar().week
            df['published_day'] = df['published_at'].dt.day
            
            # Удаляем вакансии с некорректными датами
            initial_count = len(df)
            df = df[df['published_at'].notna()]
            removed_count = initial_count - len(df)
            
            if removed_count > 0:
                self.logger.info(f"Удалено вакансий с некорректными датами: {removed_count}")
                
        return df
        
    def _clean_categorical_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очистка категориальных полей."""
        # Опыт работы
        if 'experience.name' in df.columns:
            experience_mapping = {
                'нет опыта': 'no_experience',
                'от 1 года до 3 лет': '1-3_years', 
                'от 3 до 6 лет': '3-6_years',
                'более 6 лет': '6+_years',
                'не имеет значения': 'any_experience'
            }
            
            df['experience_cleaned'] = df['experience.name'].map(experience_mapping)
            df['experience_cleaned'] = df['experience_cleaned'].fillna('other_experience')
            
        # Тип занятости
        if 'employment.name' in df.columns:
            employment_mapping = {
                'полная занятость': 'full_time',
                'частичная занятость': 'part_time', 
                'проектная работа': 'project_work',
                'волонтерство': 'volunteer',
                'стажировка': 'internship'
            }
            
            df['employment_cleaned'] = df['employment.name'].map(employment_mapping)
            df['employment_cleaned'] = df['employment_cleaned'].fillna('other_employment')
            
        # График работы
        if 'schedule.name' in df.columns:
            schedule_mapping = {
                'полный день': 'full_day',
                'сменный график': 'shift_schedule',
                'гибкий график': 'flexible_schedule', 
                'удаленная работа': 'remote_work',
                'вахтовый метод': 'shift_method'
            }
            
            df['schedule_cleaned'] = df['schedule.name'].map(schedule_mapping)
            df['schedule_cleaned'] = df['schedule_cleaned'].fillna('other_schedule')
            
        return df
        
    def _extract_and_clean_skills(self, df: pd.DataFrame) -> pd.DataFrame:
        """Извлечение и очистка навыков."""
        if 'key_skills' in df.columns:
            # Извлекаем названия навыков
            def extract_skill_names(skills_list):
                if isinstance(skills_list, list):
                    return [skill.get('name', '') for skill in skills_list if skill.get('name')]
                return []
                
            df['skill_names'] = df['key_skills'].apply(extract_skill_names)
            
            # Количество навыков
            df['skills_count'] = df['skill_names'].apply(len)
            
            # Флаг наличия навыков
            df['has_skills'] = df['skills_count'] > 0
            
            self.logger.info(f"Навыки извлечены. Вакансий с навыками: {df['has_skills'].sum()}")
            
        return df
        
    def _create_new_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создание новых признаков для анализа."""
        # Классификация уровня позиции по названию
        def classify_position_level(job_title):
            if not isinstance(job_title, str):
                return 'unknown'
                
            title_lower = job_title.lower()
            
            level_keywords = {
                'worker': ['рабочий', 'оператор', 'грузчик', 'слесарь', 'токарь'],
                'specialist': ['специалист', 'менеджер', 'технолог', 'мастер'],
                'engineer': ['инженер', 'конструктор', 'проектировщик'],
                'leadership': ['начальник', 'руководитель', 'директор', 'заведующий'],
                'executive': ['генеральный', 'директор по развитию', 'технический директор']
            }
            
            for level, keywords in level_keywords.items():
                for keyword in keywords:
                    if keyword in title_lower:
                        return level
                        
            return 'other'
            
        df['position_level'] = df['name'].apply(classify_position_level)
        
        # Классификация отраслевого сегмента
        def classify_industry_segment(job_title):
            if not isinstance(job_title, str):
                return 'other'
                
            title_lower = job_title.lower()
            
            segment_keywords = {
                'machinery': ['машиностроение', 'станкостроение', 'автомобилестроение'],
                'metallurgy': ['металлург', 'сталевар', 'прокат', 'литейщ'],
                'chemical': ['химик', 'лаборант', 'технолог хими', 'нефтехим'],
                'energy': ['энергетик', 'электрик', 'электромонтер', 'энерго'],
                'oil_gas': ['нефть', 'газ', 'буровик', 'нефтяник'],
                'construction': ['строитель', 'монтажник', 'отделочник']
            }
            
            for segment, keywords in segment_keywords.items():
                for keyword in keywords:
                    if keyword in title_lower:
                        return segment
                        
            return 'other_industry'
            
        df['industry_segment'] = df['name'].apply(classify_industry_segment)
        
        # Длина описания (прокси для сложности вакансии)
        def calculate_description_length(row):
            requirement = str(row.get('snippet.requirement', ''))
            responsibility = str(row.get('snippet.responsibility', ''))
            return len(requirement) + len(responsibility)
            
        df['description_length'] = df.apply(calculate_description_length, axis=1)
        
        # Флаг премиум вакансии
        df['is_premium'] = df.get('premium', False)
        
        self.logger.info("Новые признаки созданы")
        return df
        
    def _remove_unnecessary_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Удаление ненужных столбцов."""
        # Столбцы для удаления (сырые или дублирующиеся)
        columns_to_drop = [
            'salary.from', 'salary.to', 'salary.currency', 'salary.gross',
            'experience.name', 'employment.name', 'schedule.name',
            'premium', 'response_letter_required', 'response_url',
            'sort_point_distance', 'type.name', 'has_test', 'alternate_url',
            'apply_alternate_url', 'relations', 'snippet.requirement',
            'snippet.responsibility', 'key_skills'
        ]
        
        # Удаляем только существующие столбцы
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        
        if existing_columns_to_drop:
            df = df.drop(columns=existing_columns_to_drop)
            self.logger.info(f"Удалено столбцов: {len(existing_columns_to_drop)}")
            
        return df
        
    def save_cleaned_data(self, df: pd.DataFrame, output_path: str):
        """
        Сохранение очищенных данных.
        
        Args:
            df: Очищенный DataFrame
            output_path: Путь для сохранения
        """
        try:
            # Создаем копию DataFrame для сохранения с исправленными типами данных
            df_to_save = df.copy()
            
            # Конвертируем numpy типы в нативные Python типы для JSON
            for col in df_to_save.columns:
                if pd.api.types.is_integer_dtype(df_to_save[col]):
                    # Используем Int64 который поддерживает NaN
                    df_to_save[col] = df_to_save[col].astype('object')
                    df_to_save[col] = df_to_save[col].where(df_to_save[col].notna(), None)
                elif pd.api.types.is_float_dtype(df_to_save[col]):
                    df_to_save[col] = df_to_save[col].astype('float64')
                elif pd.api.types.is_datetime64_any_dtype(df_to_save[col]):
                    # Конвертируем datetime в строки для JSON
                    df_to_save[col] = df_to_save[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif df_to_save[col].dtype == 'object':
                    # Для object columns заменяем NaN на None
                    df_to_save[col] = df_to_save[col].where(df_to_save[col].notna(), None)
            
            # Сохраняем в разных форматах
            df_to_save.to_csv(f"{output_path}.csv", index=False, encoding='utf-8')
            df_to_save.to_parquet(f"{output_path}.parquet", index=False)
            
            # Для JSON конвертируем специальным образом
            json_data = df_to_save.to_dict('records')
            
            # Функция для сериализации специальных типов
            def json_serializer(obj):
                if isinstance(obj, (np.integer, np.int64)):
                    return int(obj)
                elif isinstance(obj, (np.floating, np.float64)):
                    return float(obj)
                elif isinstance(obj, (np.bool_, bool)):
                    return bool(obj)
                elif pd.isna(obj):
                    return None
                elif isinstance(obj, pd.Timestamp):
                    return obj.strftime('%Y-%m-%d %H:%M:%S')
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            with open(f"{output_path}.json", 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2, default=json_serializer)
            
            self.logger.info(f"Очищенные данные сохранены в: {output_path}.*")
            
            # Сохраняем описание датасета
            self._save_dataset_description(df_to_save, output_path)
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения данных: {e}")
            # Сохраняем хотя бы в CSV и Parquet
            try:
                df.to_csv(f"{output_path}.csv", index=False, encoding='utf-8')
                df.to_parquet(f"{output_path}.parquet", index=False)
                self.logger.info(f"Данные сохранены в CSV и Parquet форматах (без JSON)")
            except Exception as e2:
                self.logger.error(f"Критическая ошибка сохранения: {e2}")
            
    def _save_dataset_description(self, df: pd.DataFrame, base_path: str):
        """Сохранение описания датасета."""
        description = {
            "dataset_info": {
                "total_vacancies": len(df),
                "columns_count": len(df.columns),
                "data_types": dict(df.dtypes.astype(str)),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2
            },
            "cleaning_report": {
                "vacancies_with_salary": df['has_salary'].sum(),
                "vacancies_with_skills": df['has_skills'].sum() if 'has_skills' in df.columns else 0,
                "date_range": {
                    "start": df['published_at'].min().isoformat() if 'published_at' in df.columns and pd.notna(df['published_at'].min()) else None,
                    "end": df['published_at'].max().isoformat() if 'published_at' in df.columns and pd.notna(df['published_at'].max()) else None
                }
            },
            "column_descriptions": self._generate_column_descriptions(df)
        }
        
        with open(f"{base_path}_description.json", 'w', encoding='utf-8') as f:
            json.dump(description, f, ensure_ascii=False, indent=2)
            
    def _generate_column_descriptions(self, df: pd.DataFrame) -> Dict:
        """Генерация описаний столбцов."""
        descriptions = {
            "id": "Уникальный идентификатор вакансии",
            "name": "Название вакансии",
            "area.name": "Регион размещения вакансии",
            "employer.name": "Название компании-работодателя",
            "salary_from_rub": "Зарплата от (в рублях)",
            "salary_to_rub": "Зарплата до (в рублях)", 
            "salary_avg_rub": "Средняя зарплата (в рублях)",
            "has_salary": "Флаг наличия информации о зарплате",
            "published_at": "Дата и время публикации вакансии",
            "experience_cleaned": "Требуемый опыт работы (стандартизированный)",
            "employment_cleaned": "Тип занятости (стандартизированный)",
            "schedule_cleaned": "График работы (стандартизированный)",
            "skill_names": "Список навыков (скиллов)",
            "skills_count": "Количество навыков",
            "has_skills": "Флаг наличия навыков",
            "position_level": "Уровень позиции (классификация)",
            "industry_segment": "Отраслевой сегмент (классификация)",
            "description_length": "Длина описания вакансии (символов)",
            "is_premium": "Флаг премиум-вакансии"
        }
        
        # Фильтруем только существующие столбцы
        existing_descriptions = {col: desc for col, desc in descriptions.items() if col in df.columns}
        
        return existing_descriptions


# Функция для полной обработки датасета
def process_complete_dataset(input_file: str, output_base_name: str = None):
    """
    Полная обработка датасета: загрузка, очистка, сохранение.
    
    Args:
        input_file: Путь к сырому JSON файлу
        output_base_name: Базовое имя для выходных файлов
    """
    if not output_base_name:
        output_base_name = f"cleaned_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    cleaner = DataCleaner()
    
    # Загрузка сырых данных
    raw_data = cleaner.load_raw_data(input_file)
    
    if not raw_data:
        print("[X] Не удалось загрузить данные")
        return
        
    # Конвертация в DataFrame
    df = cleaner.convert_to_dataframe(raw_data)
    
    # Очистка данных
    cleaned_df = cleaner.clean_vacancies_dataframe(df)
    
    # Сохранение очищенных данных
    output_path = f"data/processed/{output_base_name}"
    cleaner.save_cleaned_data(cleaned_df, output_path)
    
    print(f"[V] Обработка завершена. Очищенные данные сохранены в: {output_path}.*")
    
    return cleaned_df


if __name__ == "__main__":
    # Пример использования
    process_complete_dataset("data/complete\complete_dataset_20251030_225515/combined_vacancies.json")