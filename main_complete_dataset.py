"""
Главный скрипт для формирования полного датасета.
Объединяет сбор, очистку, обогащение и валидацию данных.
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional  # ДОБАВЬТЕ ЭТИ ИМПОРТЫ

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from api.hh_api_client import HHAPIClient, collect_full_industrial_dataset
from data.data_cleaner import DataCleaner, process_complete_dataset
from data.data_validator import DataValidator, validate_complete_dataset
from database.db_manager import DatabaseManager

def main():
    """
    Основная функция для формирования полного датасета.
    """
    print("=" * 60)
    print("ФОРМИРОВАНИЕ ПОЛНОГО ДАТАСЕТА ВАКАНСИЙ")
    print("=" * 60)
    
    # Создаем директории
    os.makedirs("data/complete", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("reports/validation", exist_ok=True)
    os.makedirs("reports/final", exist_ok=True)
    
    # Параметры сбора данных
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")  # 2 года
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # ЭТАП 1: СБОР ДАННЫХ
        print("\n ЭТАП 1: СБОР ДАННЫХ")
        print("-" * 40)
        
        client = HHAPIClient()
        
        # Определяем поисковые запросы для промышленности
        search_queries = [
            "промышленность",
            "производство",
            "инженер промышленность", 
            "рабочий промышленность",
            "технолог промышленность",
            "машиностроение",
            "металлургия",
            "химическая промышленность",
            "нефтегазовая промышленность",
            "энергетика",
            "строительство промышленность",
            "авиационная промышленность",
            "судостроение",
            "станкостроение",
            "приборостроение"
        ]
        
        # Промышленные регионы России
        industrial_areas = [
            113,  # Россия
            1,    # Москва
            2,    # Санкт-Петербург
            66,   # Екатеринбург
            76,   # Нижний Новгород
            1202, # Новосибирск
            104,  # Ростов-на-Дону
            72,   # Самара
            54,   # Красноярск
            99,   # Пермь
            88,   # Воронеж
            26,   # Иркутск
        ]
        
        print(f"Период сбора: {start_date} - {end_date}")
        print(f"Поисковые запросы: {len(search_queries)}")
        print(f"Регионы: {len(industrial_areas)}")
        
        # Собираем полный датасет
        complete_dataset = client.collect_complete_dataset(
            search_queries=search_queries,
            start_date=start_date,
            end_date=end_date,
            areas=industrial_areas,
            max_vacancies_per_query=5000
        )
        
        # Сохраняем сырые данные
        dataset_dir = client.save_complete_dataset(
            complete_dataset, 
            f"industrial_vacancies_full_{timestamp}"
        )
        
        print(f"[V] Сбор данных завершен")
        print(f" Сырые данные сохранены в: {dataset_dir}")
        
        # ЭТАП 2: ОБЪЕДИНЕНИЕ И ОЧИСТКА ДАННЫХ
        print("\n ЭТАП 2: ОБЪЕДИНЕНИЕ И ОЧИСТКА ДАННЫХ")
        print("-" * 40)
        
        # Загружаем объединенные данные
        combined_file = f"{dataset_dir}/combined_vacancies.json"
        
        if not os.path.exists(combined_file):
            print("[X] Объединенный файл не найден")
            return
            
        # Обрабатываем данные
        cleaner = DataCleaner()
        raw_data = cleaner.load_raw_data(combined_file)
        
        if not raw_data:
            print("[X] Не удалось загрузить сырые данные")
            return
            
        print(f" Загружено сырых вакансий: {len(raw_data)}")
        
        # Конвертируем в DataFrame и очищаем
        df_raw = cleaner.convert_to_dataframe(raw_data)
        df_cleaned = cleaner.clean_vacancies_dataframe(df_raw)
        
        # Сохраняем очищенные данные
        cleaned_output = f"data/processed/industrial_vacancies_cleaned_{timestamp}"
        cleaner.save_cleaned_data(df_cleaned, cleaned_output)
        
        print(f"[V] Очистка данных завершена")
        print(f" Очищенные данные сохранены в: {cleaned_output}.*")
        
        # ЭТАП 3: ВАЛИДАЦИЯ ДАННЫХ
        print("\n ЭТАП 3: ВАЛИДАЦИЯ ДАННЫХ")
        print("-" * 40)
        
        # Выполняем валидацию
        validation_report = validate_complete_dataset(
            df_cleaned, 
            f"industrial_validation_{timestamp}"
        )
        
        # ЭТАП 4: СОХРАНЕНИЕ В БАЗУ ДАННЫХ
        print("\n ЭТАП 4: СОХРАНЕНИЕ В БАЗУ ДАННЫХ")
        print("-" * 40)
        
        db_manager = DatabaseManager("industrial_vacancies_complete.db")
        
        if db_manager.create_connection():
            # Создаем таблицы (если нужно)
            db_manager.create_tables()
            
            # Сохраняем данные в базу
            saved_count = 0
            for _, vacancy_row in df_cleaned.iterrows():
                # Конвертируем строку в словарь
                vacancy_dict = vacancy_row.to_dict()
                
                # Восстанавливаем структуру для сохранения
                formatted_vacancy = {
                    'id': vacancy_dict.get('id'),
                    'name': vacancy_dict.get('name'),
                    'area': {'name': vacancy_dict.get('area.name', 'Не указано')},
                    'salary': {
                        'from': vacancy_dict.get('salary_from_rub'),
                        'to': vacancy_dict.get('salary_to_rub'),
                        'currency': 'RUR'
                    },
                    'experience': {'name': vacancy_dict.get('experience_cleaned', 'Не указан')},
                    'schedule': {'name': vacancy_dict.get('schedule_cleaned', 'Не указано')},
                    'employment': {'name': vacancy_dict.get('employment_cleaned', 'Не указано')},
                    'published_at': vacancy_dict.get('published_at'),
                    'employer': {'name': vacancy_dict.get('employer.name', 'Не указано')},
                    'snippet': {
                        'requirement': '',
                        'responsibility': ''
                    },
                    'key_skills': [{'name': skill} for skill in vacancy_dict.get('skill_names', [])]
                }
                
                if db_manager.insert_vacancy(formatted_vacancy):
                    saved_count += 1
                    
                if saved_count % 100 == 0:
                    print(f"Сохранено вакансий: {saved_count}/{len(df_cleaned)}")
                    
            db_manager.close_connection()
            print(f"[V] Данные сохранены в базу: {saved_count} вакансий")
            
        # ЭТАП 5: ФОРМИРОВАНИЕ ФИНАЛЬНЫХ ОТЧЕТОВ
        print("\n ЭТАП 5: ФОРМИРОВАНИЕ ФИНАЛЬНЫХ ОТЧЕТОВ")
        print("-" * 40)
        
        # Создаем финальный отчет о датасете
        final_report = generate_final_dataset_report(
            df_cleaned, validation_report, timestamp
        )
        
        # Сохраняем финальный отчет
        report_path = f"reports/final/dataset_report_{timestamp}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
            
        print(f"[V] Финальный отчет сохранен: {report_path}")
        
        # ВЫВОД ИТОГОВ
        print("\n" + "=" * 60)
        print(" ПОЛНЫЙ ДАТАСЕТ УСПЕШНО СФОРМИРОВАН!")
        print("=" * 60)
        
        # Сводная информация
        summary = validation_report.get("summary", {})
        quality_metrics = validation_report.get("quality_checks", {})
        
        print(f" ОБЩАЯ СТАТИСТИКА:")
        print(f"   • Всего вакансий: {len(df_cleaned):,}")
        print(f"   • Качество данных: {summary.get('overall_score', 0)}%")
        print(f"   • Полнота данных: {summary.get('data_completeness', 0)}%")
        
        if 'has_salary' in df_cleaned.columns:
            salary_count = df_cleaned['has_salary'].sum()
            print(f"   • Вакансий с зарплатой: {salary_count:,} ({salary_count/len(df_cleaned)*100:.1f}%)")
            
        if 'has_skills' in df_cleaned.columns:
            skills_count = df_cleaned['has_skills'].sum()
            print(f"   • Вакансий с навыками: {skills_count:,} ({skills_count/len(df_cleaned)*100:.1f}%)")
            
        print(f"\n СОЗДАННЫЕ ФАЙЛЫ:")
        print(f"   • Сырые данные: {dataset_dir}/")
        print(f"   • Очищенные данные: {cleaned_output}.*")
        print(f"   • База данных: industrial_vacancies_complete.db")
        print(f"   • Отчеты: reports/final/dataset_report_{timestamp}.md")
        
        print(f"\n ДАТАСЕТ ГОТОВ ДЛЯ АНАЛИЗА И МОДЕЛИРОВАНИЯ!")
        
    except Exception as e:
        print(f"[X] Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()

def generate_final_dataset_report(df: pd.DataFrame, validation_report: Dict, timestamp: str) -> str:
    """
    Генерация финального отчета о датасете.
    
    Args:
        df: Очищенный DataFrame
        validation_report: Отчет о валидации
        timestamp: Метка времени
        
    Returns:
        str: Текст отчета
    """
    report = f"""# ОТЧЕТ О ПОЛНОМ ДАТАСЕТЕ ВАКАНСИЙ ПРОМЫШЛЕННОСТИ

**Дата формирования:** {datetime.now().strftime("%Y-%m-%d %H:%M")}  
**Метка времени:** {timestamp}  
**Период данных:** {(datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")} - {datetime.now().strftime("%Y-%m-%d")}

##  ОБЩАЯ СТАТИСТИКА

- **Всего вакансий:** {len(df):,}
- **Уникальных работодателей:** {df['employer.name'].nunique() if 'employer.name' in df.columns else 'N/A':,}
- **Регионов:** {df['area.name'].nunique() if 'area.name' in df.columns else 'N/A':,}
- **Период сбора:** {df['published_at'].min().strftime('%Y-%m-%d') if 'published_at' in df.columns else 'N/A'} - {df['published_at'].max().strftime('%Y-%m-%d') if 'published_at' in df.columns else 'N/A'}

##  КАЧЕСТВО ДАННЫХ

"""
    
    # Добавляем информацию о качестве
    summary = validation_report.get("summary", {})
    report += f"- **Общий score качества:** {summary.get('overall_score', 0)}%\n"
    report += f"- **Полнота данных:** {summary.get('data_completeness', 0)}%\n"
    report += f"- **Пройдено проверок:** {summary.get('passed_checks', 0)}/{summary.get('total_checks', 0)}\n\n"
    
    # Статистика по данным
    report += "##  СТАТИСТИКА ПО ДАННЫМ\n\n"
    
    if 'has_salary' in df.columns:
        salary_count = df['has_salary'].sum()
        report += f"- **Вакансий с зарплатой:** {salary_count:,} ({salary_count/len(df)*100:.1f}%)\n"
        
    if 'has_skills' in df.columns:
        skills_count = df['has_skills'].sum()
        report += f"- **Вакансий с навыками:** {skills_count:,} ({skills_count/len(df)*100:.1f}%)\n"
        
    if 'salary_avg_rub' in df.columns:
        salary_data = df['salary_avg_rub'].dropna()
        if len(salary_data) > 0:
            report += f"- **Средняя зарплата:** {salary_data.mean():,.0f} руб\n"
            report += f"- **Медианная зарплата:** {salary_data.median():,.0f} руб\n"
            
    # Распределение по уровням позиций
    if 'position_level' in df.columns:
        level_counts = df['position_level'].value_counts()
        report += "\n## 👥 РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ ПОЗИЦИЙ\n\n"
        for level, count in level_counts.items():
            percentage = (count / len(df)) * 100
            report += f"- **{level}:** {count:,} ({percentage:.1f}%)\n"
            
    # Распределение по отраслевым сегментам
    if 'industry_segment' in df.columns:
        segment_counts = df['industry_segment'].value_counts()
        report += "\n## 🏭 РАСПРЕДЕЛЕНИЕ ПО ОТРАСЛЕВЫМ СЕГМЕНТАМ\n\n"
        for segment, count in segment_counts.head(10).items():
            percentage = (count / len(df)) * 100
            report += f"- **{segment}:** {count:,} ({percentage:.1f}%)\n"
            
    # Рекомендации
    recommendations = validation_report.get("recommendations", [])
    if recommendations:
        report += "\n##  РЕКОМЕНДАЦИИ\n\n"
        for i, rec in enumerate(recommendations, 1):
            report += f"{i}. {rec}\n"
            
    # Информация о файлах
    report += f"""
##  ИНФОРМАЦИЯ О ФАЙЛАХ

Датасет сохранен в нескольких форматах:

1. **CSV** (`{timestamp}.csv`) - для анализа в Excel и других инструментах
2. **Parquet** (`{timestamp}.parquet`) - оптимальный формат для анализа в Python
3. **JSON** (`{timestamp}.json`) - для веб-приложений и обмена данными
4. **SQLite** (`industrial_vacancies_complete.db`) - реляционная база данных

##  ВОЗМОЖНОСТИ ДЛЯ АНАЛИЗА

Собранный датасет позволяет провести:

- Анализ рынка труда в промышленности
- Сравнение зарплат по регионам и специальностям
- Выявление наиболее востребованных навыков
- Прогнозирование спроса на специалистов
- Анализ динамики рынка труда


---
*Сформировано автоматически системой сбора данных о вакансиях промышленности*
"""
    
    return report

if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Формирование полного датасета вакансий промышленности')
    parser.add_argument('--skip-collection', action='store_true', 
                       help='Пропустить сбор данных (использовать существующие)')
    parser.add_argument('--input-file', type=str, 
                       help='Путь к существующему файлу с данными')
    
    args = parser.parse_args()
    
    if args.skip_collection and args.input_file:
        # Обработка существующих данных
        print(" Обработка существующих данных...")
        df = pd.read_parquet(args.input_file)
        validation_report = validate_complete_dataset(df)
        final_report = generate_final_dataset_report(df, validation_report, 
                                                   datetime.now().strftime("%Y%m%d_%H%M%S"))
        
        report_path = f"reports/final/dataset_report_existing.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(final_report)
            
        print(f"[V] Отчет сохранен: {report_path}")
    else:
        # Полный процесс сбора и обработки
        main()