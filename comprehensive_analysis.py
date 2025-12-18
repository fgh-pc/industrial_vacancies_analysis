# comprehensive_analysis.py
"""
КОМПЛЕКСНЫЙ АНАЛИЗ ПРОМЫШЛЕННЫХ ВАКАНСИЙ С ГРАФИКАМИ И ТЕКСТОВЫМ ОТЧЕТОМ
Использует модульную структуру анализа
"""

import sqlite3
import os
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

# Импортируем модули анализа
from analysis_modules import (
    analyze_industry_segments,
    analyze_position_levels,
    analyze_salary_comparison,
    analyze_dynamics,
    analyze_professions_dynamics,
    analyze_regional_distribution,
    analyze_skills,
    analyze_forecast,
    analyze_dashboard,
    save_text_report
)

# Настройка стиля графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class ComprehensiveIndustrialAnalyzer:
    """
    Комплексный анализатор с визуализацией и текстовым отчетом.
    """
    
    def __init__(self, db_path: str = "industrial_vacancies.db"):
        self.db_path = db_path
        self.connection = None
        self.report_data = {}
        
        # Нормализуем путь для корректной работы в Windows
        self.output_dir = os.path.normpath("reports/comprehensive_analysis")
        
        # Создаем директорию для результатов
        os.makedirs(self.output_dir, exist_ok=True)
        
    def connect_to_database(self) -> bool:
        """Подключение к базе данных."""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            print("✅ Подключение к базе данных установлено")
            return True
        except sqlite3.Error as e:
            print(f"❌ Ошибка подключения: {e}")
            return False

    def create_industry_segments_chart(self):
        """График 1: Распределение по отраслевым сегментам."""
        result = analyze_industry_segments(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_position_levels_chart(self):
        """График 2: Распределение по уровням позиций."""
        result = analyze_position_levels(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_salary_comparison_chart(self):
        """График 3: Сравнение зарплат по категориям."""
        result = analyze_salary_comparison(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_dynamics_chart(self):
        """График 4: Динамика спроса по месяцам."""
        result = analyze_dynamics(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_professions_dynamics_chart(self):
        """График 4a: Динамика изменения спроса на различные промышленные профессии."""
        result = analyze_professions_dynamics(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_regional_distribution_chart(self):
        """График 5: Региональное распределение."""
        result = analyze_regional_distribution(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_skills_analysis_chart(self):
        """График 6: Анализ востребованных навыков."""
        result = analyze_skills(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_forecast_chart(self):
        """График 7: Прогноз спроса на следующий год."""
        result = analyze_forecast(self.connection, self.output_dir)
        self.report_data.update(result)

    def create_summary_dashboard(self):
        """Создает сводный дашборд с ключевыми метриками."""
        result = analyze_dashboard(self.connection, self.output_dir)
        self.report_data.update(result)

    def save_text_report(self):
        """Сохраняет текстовый отчет."""
        save_text_report(self.report_data, self.output_dir, self.db_path)

    def generate_all_charts_and_report(self):
        """Генерирует все графики и отчет."""
        print("🚀 ЗАПУСК КОМПЛЕКСНОГО АНАЛИЗА С ГРАФИКАМИ")
        print("=" * 60)
        
        if not self.connect_to_database():
            return
        
        # Создаем все графики
        self.create_industry_segments_chart()
        self.create_position_levels_chart()
        self.create_salary_comparison_chart()
        self.create_dynamics_chart()
        self.create_professions_dynamics_chart()  # Новый график динамики по профессиям
        self.create_regional_distribution_chart()
        self.create_skills_analysis_chart()
        self.create_forecast_chart()
        self.create_summary_dashboard()
        
        # Сохраняем отчет
        self.save_text_report()
        
        self.connection.close()
        print("✅ Соединение с базой данных закрыто")
        
        # Выводим итоговую информацию
        self.print_final_summary()

    def check_salary_range(self):
        """Проверяет минимальную и максимальную зарплату."""
        try:
            import pandas as pd
            
            query = """
                SELECT 
                    MIN(salary_avg_rub) as min_salary,
                    MAX(salary_avg_rub) as max_salary,
                    AVG(salary_avg_rub) as avg_salary,
                    COUNT(*) as total
                FROM vacancies 
                WHERE is_industrial = 1 
                AND has_salary = 1 
                AND salary_avg_rub IS NOT NULL
                AND salary_avg_rub > 0
            """
            
            df = pd.read_sql_query(query, self.connection)
            
            if not df.empty and df.iloc[0]['total'] > 0:
                min_salary = int(df.iloc[0]['min_salary'])
                max_salary = int(df.iloc[0]['max_salary'])
                avg_salary = int(df.iloc[0]['avg_salary'])
                total = int(df.iloc[0]['total'])
                
                print("\n" + "=" * 80)
                print("💰 ЗАРПЛАТЫ В ПРОМЫШЛЕННЫХ ВАКАНСИЯХ")
                print("=" * 80)
                print(f"\nВсего вакансий с указанной зарплатой: {total:,}")
                print(f"\nМинимальная зарплата: {min_salary:,} руб")
                print(f"Максимальная зарплата: {max_salary:,} руб")
                print(f"Средняя зарплата: {avg_salary:,} руб")
                print("=" * 80 + "\n")
            else:
                print("Нет данных о зарплатах в базе данных")
                
        except Exception as e:
            print(f"❌ Ошибка при проверке зарплат: {e}")

    def print_final_summary(self):
        """Выводит итоговую сводку."""
        print("\n" + "=" * 60)
        print("🎉 КОМПЛЕКСНЫЙ АНАЛИЗ ЗАВЕРШЕН!")
        print("=" * 60)
        
        metrics = self.report_data.get('summary_metrics', {})
        
        print(f"📊 ОСНОВНЫЕ МЕТРИКИ:")
        print(f"   • Промышленных вакансий: {metrics.get('total_vacancies', 0):,}")
        print(f"   • Охват зарплатами: {metrics.get('salary_coverage', 0)}%")
        print(f"   • Средняя зарплата: {metrics.get('avg_salary', 0):,} руб")
        print(f"   • Работодателей: {metrics.get('unique_employers', 0):,}")
        print(f"   • Регионов: {metrics.get('unique_regions', 0)}")
        
        print(f"\n📁 РЕЗУЛЬТАТЫ:")
        print(f"   • Создано графиков: 9")
        print(f"   • Папка с результатами: {self.output_dir}/")
        print(f"   • Текстовый отчет: {self.output_dir}/comprehensive_analysis_report.txt")
        
        print(f"\n📈 КЛЮЧЕВЫЕ ГРАФИКИ:")
        charts = [
            "01_industry_segments.png - Распределение по отраслям",
            "02_position_levels.png - Уровни позиций и зарплаты", 
            "03_salary_comparison.png - Сравнение зарплат",
            "04_dynamics.png - Динамика спроса",
            "04_professions_dynamics.png - Динамика по профессиям",
            "05_regional_distribution.png - Региональное распределение",
            "06_skills_analysis.png - Востребованные навыки",
            "07_forecast.png - Прогноз спроса",
            "08_summary_dashboard.png - Сводный дашборд"
        ]
        
        for chart in charts:
            print(f"   • {chart}")


# Запуск комплексного анализа
if __name__ == "__main__":
    import sys
    analyzer = ComprehensiveIndustrialAnalyzer()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--check-salary':
        # Режим проверки зарплат
        analyzer.connect_to_database()
        analyzer.check_salary_range()
        analyzer.connection.close()
    else:
        # Обычный режим - полный анализ
        analyzer.generate_all_charts_and_report()