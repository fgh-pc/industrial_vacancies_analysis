"""
Главный скрипт для формирования полного датасета промышленных вакансий.
Объединяет сбор, очистку, обогащение и валидацию данных.
Оптимизирован для работы с асинхронным API и большими объемами данных.
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional, Any
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from src.api.hh_api_client import MassHHCollector
    from data.data_cleaner import DataCleaner
    from data.data_validator import DataValidator
    from database.db_manager import DatabaseManager
        # ФИКС: Создаем алиас для обратной совместимости
    TurboHHAPIClient = MassHHCollector

except ImportError as e:
    logger.error(f"Ошибка импорта модулей: {e}")
    sys.exit(1)


class IndustrialDatasetBuilder:
    """
    Класс для построения полного датасета промышленных вакансий.
    """
    
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.setup_directories()
        
    def setup_directories(self):
        """Создает необходимые директории."""
        directories = [
            "data/complete",
            "data/processed", 
            "data/raw",
            "reports/validation",
            "reports/final",
            "logs"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    async def collect_data(self, target_count: int = 5000000) -> Dict[str, Any]:
        """
        Асинхронный сбор данных через TurboHHAPIClient.
        
        Args:
            target_count: Целевое количество вакансий
            
        Returns:
            Dict с результатами сбора
        """
        logger.info(" Начинаем сбор данных...")
        
        try:
            client = TurboHHAPIClient()
            
            # Запускаем турбо-сбор
            results = await client.mass_collect()
            
            # Сохраняем сырые данные
            raw_data_dir = f"data/raw/industrial_raw_{self.timestamp}"
            os.makedirs(raw_data_dir, exist_ok=True)
            
            # Сохраняем результаты по запросам
            for query_key, vacancies in results.items():
                if vacancies:
                    filename = f"{raw_data_dir}/{query_key.replace(' ', '_')}.json"
                    # Здесь нужно добавить сохранение в JSON
                    pass
            
            await client.close()
            
            return {
                'success': True,
                'total_collected': client.total_collected,
                'raw_data_dir': raw_data_dir,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"[X] Ошибка при сборе данных: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def process_data(self, raw_results: Dict) -> Optional[pd.DataFrame]:
        """
        Обработка и очистка собранных данных.
        
        Args:
            raw_results: Результаты сбора данных
            
        Returns:
            Очищенный DataFrame или None при ошибке
        """
        logger.info(" Начинаем обработку данных...")
        
        try:
            # Собираем все вакансии в один список
            all_vacancies = []
            for vacancies in raw_results['results'].values():
                all_vacancies.extend(vacancies)
            
            logger.info(f" Всего собрано вакансий: {len(all_vacancies):,}")
            
            if not all_vacancies:
                logger.error("[X] Нет данных для обработки")
                return None
            
            # Очистка данных
            cleaner = DataCleaner()
            df_raw = cleaner.convert_to_dataframe(all_vacancies)
            df_cleaned = cleaner.clean_vacancies_dataframe(df_raw)
            
            # Сохраняем очищенные данные
            cleaned_path = f"data/processed/industrial_cleaned_{self.timestamp}"
            cleaner.save_cleaned_data(df_cleaned, cleaned_path)
            
            logger.info(f"[V] Данные обработаны и сохранены: {cleaned_path}")
            return df_cleaned
            
        except Exception as e:
            logger.error(f"[X] Ошибка при обработке данных: {e}")
            return None
    
    async def save_to_database(self, df: pd.DataFrame) -> bool:
        """
        Сохранение данных в базу данных.
        
        Args:
            df: DataFrame с очищенными данными
            
        Returns:
            True если успешно, False при ошибке
        """
        logger.info(" Сохраняем данные в базу...")
        
        try:
            db_manager = DatabaseManager("industrial_vacancies.db")
            
            if not await db_manager.create_connection():
                logger.error("[X] Не удалось подключиться к базе данных")
                return False
            
            # Создаем таблицы если нужно
            if not await db_manager.tables_exist():
                logger.info(" Создаем таблицы в базе данных...")
                if not await db_manager.create_tables_async():
                    logger.error("[X] Не удалось создать таблицы")
                    return False
            
            # Конвертируем DataFrame в список словарей для батчевой вставки
            vacancies_to_insert = []
            for _, row in df.iterrows():
                vacancy_dict = self._prepare_vacancy_for_db(row)
                if vacancy_dict:
                    vacancies_to_insert.append(vacancy_dict)
            
            # Массовая вставка
            inserted_count = await db_manager.insert_vacancy_batch(vacancies_to_insert)
            
            await db_manager.close_connection()
            
            logger.info(f"[V] В базу сохранено {inserted_count} вакансий")
            return inserted_count > 0
            
        except Exception as e:
            logger.error(f"[X] Ошибка при сохранении в базу: {e}")
            return False
    
    def _prepare_vacancy_for_db(self, row: pd.Series) -> Optional[Dict]:
        """
        Подготавливает данные вакансии для сохранения в БД.
        
        Args:
            row: Строка DataFrame
            
        Returns:
            Словарь с данными вакансии
        """
        try:
            vacancy_id = row.get('id')
            if not vacancy_id:
                return None
            
            # Базовые поля
            vacancy = {
                'id': vacancy_id,
                'name': row.get('name', ''),
                'area': {'name': row.get('area', 'Не указано')},
                'salary': {
                    'from': row.get('salary_from'),
                    'to': row.get('salary_to'),
                    'currency': row.get('salary_currency', 'RUR')
                },
                'experience': {'name': row.get('experience', 'Не указан')},
                'schedule': {'name': row.get('schedule', 'Не указано')},
                'employment': {'name': row.get('employment', 'Не указано')},
                'published_at': row.get('published_at'),
                'employer': {'name': row.get('employer_name', 'Не указано')},
                'snippet': {
                    'requirement': row.get('snippet_requirement', ''),
                    'responsibility': row.get('snippet_responsibility', '')
                }
            }
            
            # Навыки
            skills = []
            if 'skills' in row and isinstance(row['skills'], list):
                skills = [{'name': skill} for skill in row['skills']]
            elif 'skill_names' in row and isinstance(row['skill_names'], list):
                skills = [{'name': skill} for skill in row['skill_names']]
            
            vacancy['key_skills'] = skills
            
            return vacancy
            
        except Exception as e:
            logger.warning(f"[!] Ошибка подготовки вакансии {row.get('id')}: {e}")
            return None
    
    def validate_dataset(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Валидация датасета.
        
        Args:
            df: DataFrame для валидации
            
        Returns:
            Отчет о валидации
        """
        logger.info(" Выполняем валидацию данных...")
        
        try:
            validator = DataValidator()
            validation_report = validator.validate_complete_dataset(
                df, 
                f"industrial_validation_{self.timestamp}"
            )
            
            logger.info("[V] Валидация завершена")
            return validation_report
            
        except Exception as e:
            logger.error(f"[X] Ошибка при валидации: {e}")
            return {}
    
    def generate_final_report(self, df: pd.DataFrame, validation_report: Dict, 
                            collection_stats: Dict) -> str:
        """
        Генерация финального отчета.
        
        Args:
            df: Очищенный DataFrame
            validation_report: Отчет валидации
            collection_stats: Статистика сбора
            
        Returns:
            Текст отчета
        """
        logger.info(" Генерируем финальный отчет...")
        
        # Базовая информация
        total_vacancies = len(df)
        unique_employers = df['employer_name'].nunique() if 'employer_name' in df.columns else 0
        unique_regions = df['area'].nunique() if 'area' in df.columns else 0
        
        # Статистика по зарплатам
        salary_stats = ""
        if 'salary_avg' in df.columns:
            salary_data = df['salary_avg'].dropna()
            if len(salary_data) > 0:
                salary_stats = f"""
- **Средняя зарплата:** {salary_data.mean():,.0f} руб
- **Медианная зарплата:** {salary_data.median():,.0f} руб
- **Минимальная зарплата:** {salary_data.min():,.0f} руб  
- **Максимальная зарплата:** {salary_data.max():,.0f} руб
"""
        
        report = f"""#  ОТЧЕТ О ПОЛНОМ ДАТАСЕТЕ ПРОМЫШЛЕННЫХ ВАКАНСИЙ

**Дата формирования:** {datetime.now().strftime("%Y-%m-%d %H:%M")}  
**Метка времени:** {self.timestamp}  
**Целевой объем:** {collection_stats.get('target_count', 0):,} вакансий

##  ОБЩАЯ СТАТИСТИКА

- **Всего вакансий:** {total_vacancies:,}
- **Уникальных работодателей:** {unique_employers:,}
- **Регионов:** {unique_regions:,}
- **Собрано за сессию:** {collection_stats.get('collected', 0):,}

##  СТАТИСТИКА ПО ЗАРПЛАТАМ

{salary_stats}

##  КАЧЕСТВО ДАННЫХ

- **Общий score качества:** {validation_report.get('overall_score', 0)}%
- **Полнота данных:** {validation_report.get('completeness_score', 0)}%
- **Пройдено проверок:** {validation_report.get('passed_checks', 0)}/{validation_report.get('total_checks', 0)}

##  СОЗДАННЫЕ ФАЙЛЫ

- **Сырые данные:** data/raw/industrial_raw_{self.timestamp}/
- **Очищенные данные:** data/processed/industrial_cleaned_{self.timestamp}.*
- **База данных:** industrial_vacancies.db
- **Логи:** data_collection.log

##  РЕКОМЕНДАЦИИ

Собранный датасет готов для:
- Анализа рынка труда в промышленности
- Сравнения зарплат по регионам и специальностям  
- Выявления востребованных навыков
- Прогнозирования спроса на специалистов
- Анализа динамики рынка труда

---
*Сформировано автоматически системой сбора данных о вакансиях промышленности*
"""
        
        return report


async def main():
    """
    Основная асинхронная функция построения датасета.
    """
    print("=" * 70)
    print(" ФОРМИРОВАНИЕ ПОЛНОГО ДАТАСЕТА ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
    print("=" * 70)
    
    builder = IndustrialDatasetBuilder()
    
    try:
        # ЭТАП 1: СБОР ДАННЫХ
        logger.info(" ЭТАП 1: СБОР ДАННЫХ")
        collection_result = await builder.collect_data(target_count=5000000)
        
        if not collection_result['success']:
            logger.error("[X] Сбор данных прерван")
            return
        
        # ЭТАП 2: ОБРАБОТКА ДАННЫХ
        logger.info(" ЭТАП 2: ОБРАБОТКА ДАННЫХ")
        df_cleaned = await builder.process_data(collection_result)
        
        if df_cleaned is None:
            logger.error("[X] Обработка данных прервана")
            return
        
        # ЭТАП 3: СОХРАНЕНИЕ В БАЗУ ДАННЫХ
        logger.info(" ЭТАП 3: СОХРАНЕНИЕ В БАЗУ ДАННЫХ")
        db_success = await builder.save_to_database(df_cleaned)
        
        if not db_success:
            logger.warning("[!] Не удалось сохранить данные в базу")
        
        # ЭТАП 4: ВАЛИДАЦИЯ
        logger.info(" ЭТАП 4: ВАЛИДАЦИЯ ДАННЫХ")
        validation_report = builder.validate_dataset(df_cleaned)
        
        # ЭТАП 5: ФИНАЛЬНЫЙ ОТЧЕТ
        logger.info(" ЭТАП 5: ФОРМИРОВАНИЕ ОТЧЕТА")
        
        collection_stats = {
            'target_count': 5000000,
            'collected': collection_result['total_collected'],
            'timestamp': builder.timestamp
        }
        
        final_report = builder.generate_final_report(
            df_cleaned, validation_report, collection_stats
        )
        
        # Сохраняем отчет
        report_path = f"reports/final/dataset_report_{builder.timestamp}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
        
        # ВЫВОД ИТОГОВ
        print("\n" + "=" * 70)
        print("[V] ПОЛНЫЙ ДАТАСЕТ УСПЕШНО СФОРМИРОВАН!")
        print("=" * 70)
        print(f" Собрано вакансий: {collection_result['total_collected']:,}")
        print(f" Очищено записей: {len(df_cleaned):,}")
        print(f" Отчет сохранен: {report_path}")
        print(f" Время: {builder.timestamp}")
        print("=" * 70)
        
    except Exception as e:
        logger.error(f"[X] Критическая ошибка: {e}")
        raise


async def process_existing_data(input_file: str):
    """
    Обработка существующих данных.
    
    Args:
        input_file: Путь к файлу с данными
    """
    logger.info(f" Обрабатываем существующие данные: {input_file}")
    
    builder = IndustrialDatasetBuilder()
    
    try:
        # Загружаем данные
        if input_file.endswith('.parquet'):
            df = pd.read_parquet(input_file)
        elif input_file.endswith('.csv'):
            df = pd.read_csv(input_file)
        else:
            logger.error("[X] Неподдерживаемый формат файла")
            return
        
        # Обрабатываем данные
        validation_report = builder.validate_dataset(df)
        
        # Сохраняем в базу
        db_success = await builder.save_to_database(df)
        
        # Генерируем отчет
        collection_stats = {
            'target_count': len(df),
            'collected': len(df),
            'timestamp': builder.timestamp
        }
        
        final_report = builder.generate_final_report(
            df, validation_report, collection_stats
        )
        
        report_path = f"reports/final/dataset_report_existing_{builder.timestamp}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
        
        logger.info(f"[V] Обработка завершена. Отчет: {report_path}")
        
    except Exception as e:
        logger.error(f"[X] Ошибка обработки существующих данных: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Формирование полного датасета промышленных вакансий'
    )
    parser.add_argument(
        '--skip-collection', 
        action='store_true',
        help='Пропустить сбор данных (использовать существующие)'
    )
    parser.add_argument(
        '--input-file', 
        type=str,
        help='Путь к существующему файлу с данными'
    )
    parser.add_argument(
        '--target-count',
        type=int,
        default=5000000,
        help='Целевое количество вакансий для сбора'
    )
    
    args = parser.parse_args()
    
    if args.skip_collection and args.input_file:
        # Обработка существующих данных
        asyncio.run(process_existing_data(args.input_file))
    else:
        # Полный процесс сбора
        asyncio.run(main())