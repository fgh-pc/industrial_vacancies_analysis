"""
ОБНОВЛЕННЫЙ ГЛАВНЫЙ СКРИПТ АНАЛИЗА ДЛЯ 500K+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ
ИСПРАВЛЕННАЯ ВЕРСИЯ С ПРОВЕРКОЙ ЗАГРУЗКИ ДАННЫХ
"""

import os
import sys
import json
from datetime import datetime

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from database.db_manager import IndustrialDatabaseManager, load_industrial_data
from analysis.data_analyzer import IndustrialDataAnalyzer, run_industrial_analysis
from analysis.visualizer import IndustrialDataVisualizer

def check_database_loaded(db_manager: IndustrialDatabaseManager) -> bool:
    """
    Проверяет, загружены ли данные в базу.
    
    Returns:
        bool: True если данные загружены, False если таблицы пустые
    """
    try:
        cursor = db_manager.connection.cursor()
        
        # Проверяем наличие таблицы vacancies
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vacancies'")
        if not cursor.fetchone():
            print("❌ Таблица vacancies не существует")
            return False
        
        # Проверяем, есть ли данные в таблице
        cursor.execute("SELECT COUNT(*) as count FROM vacancies")
        vacancy_count = cursor.fetchone()[0]
        
        print(f"📊 В базе данных: {vacancy_count} вакансий")
        
        return vacancy_count > 0
        
    except Exception as e:
        print(f"❌ Ошибка проверки базы данных: {e}")
        return False

def ensure_data_loaded() -> bool:
    """
    Гарантирует, что данные загружены в базу.
    
    Returns:
        bool: True если данные успешно загружены
    """
    json_file = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
    db_file = "industrial_vacancies.db"
    
    # Создаем менеджер БД для проверки
    db_manager = IndustrialDatabaseManager(db_file)
    
    if db_manager.create_connection():
        # Проверяем, загружены ли данные
        if not check_database_loaded(db_manager):
            print("🔄 Данные не загружены, начинаем загрузку...")
            db_manager.close_connection()
            
            # Загружаем данные
            load_industrial_data()
            
            # Проверяем снова
            db_manager.create_connection()
            if check_database_loaded(db_manager):
                print("✅ Данные успешно загружены в базу")
                db_manager.close_connection()
                return True
            else:
                print("❌ Не удалось загрузить данные в базу")
                db_manager.close_connection()
                return False
        else:
            print("✅ Данные уже загружены в базу")
            db_manager.close_connection()
            return True
    else:
        print("❌ Не удалось подключиться к базе данных")
        return False

def main():
    """
    Основная функция для запуска полного анализа промышленных вакансий.
    """
    print("=" * 70)
    print("🚀 СИСТЕМА АНАЛИЗА 500K+ ПРОМЫШЛЕННЫХ ВАКАНСИЙ")
    print("🇷🇺 ТОЛЬКО РОССИЙСКИЕ РЕГИОНЫ")
    print("🔧 РАСШИРЕННАЯ ПРОМЫШЛЕННАЯ АНАЛИТИКА")
    print("=" * 70)
    
    # Создаем директории
    os.makedirs("reports", exist_ok=True)
    os.makedirs("notebooks", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    # Проверяем наличие исходных данных
    json_file = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
    
    if not os.path.exists(json_file):
        print(f"❌ Файл с данными не найден: {json_file}")
        print("💡 Сначала запустите сбор данных через hh_api_client.py")
        return
    
    print(f"📁 Найден файл с данными: {json_file}")
    
    # Шаг 1: Гарантируем загрузку данных в базу
    print("\n📥 ШАГ 1: Проверка и загрузка данных в базу данных...")
    
    if not ensure_data_loaded():
        print("❌ Не удалось загрузить данные в базу. Анализ прерван.")
        return
    
    # Шаг 2: Запуск анализа
    print("\n📊 ШАГ 2: Запуск комплексного анализа...")
    
    analyzer = IndustrialDataAnalyzer()
    if analyzer.connect_to_database():
        report = analyzer.generate_comprehensive_report()
        analyzer.close_connection()
    else:
        print("❌ Не удалось подключиться к базе данных для анализа")
        return
    
    if report:
        # Шаг 3: Сохранение отчета
        print("\n💾 ШАГ 3: Сохранение результатов...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"reports/industrial_analysis_report_{timestamp}.json"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Отчет сохранен: {report_filename}")
        
        # Шаг 4: Визуализация (с исправлением)
        print("\n🎨 ШАГ 4: Создание визуализаций...")
        
        try:
            analyzer = IndustrialDataAnalyzer()
            if analyzer.connect_to_database():
                visualizer = IndustrialDataVisualizer(analyzer)
                visualizer.create_all_visualizations()
                analyzer.close_connection()
        except Exception as e:
            print(f"⚠️  Визуализации не созданы: {e}")
        
        # Финальная статистика
        basic_stats = report.get('basic_statistics', {})
        total_vacancies = basic_stats.get('total_vacancies', 0)
        
        print("\n" + "=" * 70)
        print("✅ АНАЛИЗ ЗАВЕРШЕН УСПЕШНО!")
        print("=" * 70)
        print(f"📊 Проанализировано вакансий: {total_vacancies:,}")
        print(f"💰 Охват зарплатами: {basic_stats.get('salary_coverage_percent', 0):.1f}%")
        print(f"🌍 Регионов: {basic_stats.get('unique_regions', 0)}")
        print(f"🏢 Работодателей: {basic_stats.get('unique_employers', 0):,}")
        
        # Ключевые выводы
        print("\n🎯 ОСНОВНЫЕ РЕЗУЛЬТАТЫ:")
        findings = report.get('key_findings', [])
        if findings:
            for i, finding in enumerate(findings[:5], 1):
                print(f"  {i}. {finding}")
        else:
            print("  ℹ️  Ключевые выводы не сгенерированы")
        
        print(f"\n📁 Результаты:")
        print(f"  📄 Отчет: {report_filename}")
        print(f"  📊 База данных: industrial_vacancies.db")
        print(f"  🎨 Графики: в папке reports/")
        
    else:
        print("❌ Анализ не удался")

def check_data_quality():
    """
    Проверка качества данных перед анализом.
    """
    print("\n🔍 ПРОВЕРКА КАЧЕСТВА ДАННЫХ...")
    
    json_file = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
    
    if not os.path.exists(json_file):
        print("❌ Файл с данными не найден")
        return False
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print("❌ Неверный формат данных: ожидается список")
            return False
        
        print(f"✅ Найдено вакансий: {len(data):,}")
        
        # Базовая проверка структуры
        if data:
            sample_vacancy = data[0]
            required_fields = ['id', 'name', 'area']
            missing_fields = [field for field in required_fields if field not in sample_vacancy]
            
            if missing_fields:
                print(f"⚠️  Отсутствуют поля: {missing_fields}")
            else:
                print("✅ Структура данных корректна")
            
            # Проверка промышленных характеристик
            industrial_keywords = ['инженер', 'технолог', 'сварщик', 'оператор']
            industrial_count = sum(1 for vacancy in data[:1000]  # Проверяем выборку
                                 if any(keyword in vacancy.get('name', '').lower() 
                                       for keyword in industrial_keywords))
            
            print(f"✅ Промышленных вакансий в выборке: {industrial_count}/1000")
        else:
            print("⚠️  Файл данных пуст")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки данных: {e}")
        return False

if __name__ == "__main__":
    # Проверяем данные перед запуском
    if check_data_quality():
        main()
    else:
        print("❌ Не удалось запустить анализ из-за проблем с данными")