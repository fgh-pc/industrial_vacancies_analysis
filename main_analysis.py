"""
Главный скрипт для запуска предварительного анализа данных.
Собирает все данные, выполняет анализ и генерирует отчет.
"""

import os
import sys
from datetime import datetime

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from analysis.data_analyzer import DataAnalyzer
from analysis.visualizer import DataVisualizer, create_quick_visualizations

def main():
    """
    Основная функция для запуска анализа данных.
    """
    print("=" * 60)
    print("ПРЕДВАРИТЕЛЬНЫЙ АНАЛИЗ ВАКАНСИЙ ПРОМЫШЛЕННОСТИ")
    print("=" * 60)
    
    # Создаем директории для отчетов, если их нет
    os.makedirs("reports", exist_ok=True)
    os.makedirs("notebooks", exist_ok=True)
    
    # Инициализируем анализатор
    analyzer = DataAnalyzer()
    
    # Подключаемся к базе данных
    if not analyzer.connect_to_database():
        print("[X] Не удалось подключиться к базе данных. Завершение.")
        return
        
    try:
        # Загружаем данные
        print("\n Загружаем данные из базы...")
        analyzer.load_data_to_dataframes()
        
        if analyzer.df_vacancies is None or len(analyzer.df_vacancies) == 0:
            print("[X] В базе данных нет вакансий. Сначала соберите данные.")
            return
            
        # Предобрабатываем данные
        print("\n Выполняем предобработку данных...")
        analyzer.preprocess_data()
        
        # Базовая статистика
        print("\n Собираем базовую статистику...")
        stats = analyzer.get_basic_statistics()
        
        print("\n" + "=" * 40)
        print("ОСНОВНЫЕ РЕЗУЛЬТАТЫ АНАЛИЗА")
        print("=" * 40)
        
        print(f"• Всего вакансий: {stats.get('total_vacancies', 0):,}")
        print(f"• Вакансий с указанной зарплатой: {stats.get('vacancies_with_salary', 0):,}")
        print(f"• Уникальных работодателей: {stats.get('unique_employers', 0):,}")
        print(f"• Регионов: {stats.get('unique_regions', 0):,}")
        
        if 'avg_salary' in stats:
            print(f"• Средняя зарплата: {stats['avg_salary']:,.0f} руб")
            print(f"• Медианная зарплата: {stats['median_salary']:,.0f} руб")
        
        # Анализ отраслевых сегментов
        print("\n АНАЛИЗ ОТРАСЛЕВЫХ СЕГМЕНТОВ")
        segments = analyzer.analyze_industry_segments()
        if segments:
            top_segments = segments.get('top_segments', {})
            print("• Топ-5 сегментов по количеству вакансий:")
            for segment, count in top_segments.items():
                percentage = segments.get('percentages', {}).get(segment, 0)
                print(f"  {segment}: {count} ({percentage:.1f}%)")
        
        # Анализ уровней позиций
        print("\n АНАЛИЗ УРОВНЕЙ ПОЗИЦИЙ")
        levels = analyzer.analyze_position_levels()
        if levels:
            most_demanded = levels.get('most_demanded_level')
            most_count = levels.get('most_demanded_count')
            print(f"• Наиболее востребованный уровень: {most_demanded} ({most_count} вакансий)")
            
            distribution = levels.get('distribution', {})
            print("• Распределение по уровням:")
            for level, count in distribution.items():
                print(f"  {level}: {count}")
        
        # Анализ зарплат
        print("\n СРАВНЕНИЕ ЗАРПЛАТ")
        salaries = analyzer.analyze_salaries_comparison()
        if salaries:
            engineer_vs_worker = salaries.get('engineer_vs_worker', {})
            if engineer_vs_worker:
                eng_salary = engineer_vs_worker.get('engineer_avg_salary', 0)
                worker_salary = engineer_vs_worker.get('worker_avg_salary', 0)
                ratio = engineer_vs_worker.get('salary_ratio', 0)
                
                print(f"• Средняя зарплата инженеров: {eng_salary:,.0f} руб")
                print(f"• Средняя зарплата рабочих: {worker_salary:,.0f} руб")
                print(f"• Соотношение (инженер/рабочий): {ratio:.2f}")
        
        # Анализ динамики
        print("\n ДИНАМИКА СПРОСА")
        dynamics = analyzer.analyze_dynamics()
        if dynamics:
            growth_rate = dynamics.get('growth_rate', 0)
            monthly_data = dynamics.get('monthly', {})
            
            print(f"• Темп роста за период: {growth_rate:+.1f}%")
            print(f"• Анализировано периодов: {len(monthly_data)}")
            
            if monthly_data:
                # Последние 3 месяца
                recent_months = list(monthly_data.items())[-3:]
                print("• Последние 3 месяца:")
                for period, count in recent_months:
                    print(f"  {period}: {count} вакансий")
        
        # Анализ навыков
        print("\n АНАЛИЗ НАВЫКОВ")
        skills = analyzer.analyze_skills()
        if skills:
            top_skills = skills.get('top_skills', {})
            total_skills = skills.get('total_unique_skills', 0)
            avg_skills = skills.get('avg_skills_per_vacancy', 0)
            
            print(f"• Всего уникальных навыков: {total_skills}")
            print(f"• Среднее количество навыков на вакансию: {avg_skills:.1f}")
            print("• Топ-5 наиболее востребованных навыков:")
            
            for i, (skill, count) in enumerate(list(top_skills.items())[:5]):
                print(f"  {i+1}. {skill}: {count}")
        
        # Создаем визуализации
        print("\n СОЗДАЕМ ВИЗУАЛИЗАЦИИ...")
        visualizer = DataVisualizer(analyzer)
        
        # Создаем все основные графики
        visualizer.plot_industry_segments("reports/industry_segments.png")
        visualizer.plot_position_levels("reports/position_levels.png")
        visualizer.plot_salary_comparison("reports/salary_comparison.png")
        visualizer.plot_dynamics("reports/dynamics.png")
        visualizer.create_comprehensive_dashboard("reports/dashboard.png")
        
        # Генерируем текстовый отчет
        generate_text_report(analyzer, "reports/preliminary_report.txt")
        
        print("\n[V] АНАЛИЗ ЗАВЕРШЕН!")
        print(" Результаты сохранены в папке 'reports/'")
        print(" Графики: industry_segments.png, position_levels.png, salary_comparison.png, dynamics.png, top_skills.png, dashboard.png")
        print(" Отчет: preliminary_report.txt")
        
    except Exception as e:
        print(f"[X] Произошла ошибка во время анализа: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Закрываем соединение с базой
        analyzer.close_connection()

def generate_text_report(analyzer: DataAnalyzer, report_path: str):
    """
    Генерация текстового отчета с результатами анализа.
    
    Args:
        analyzer (DataAnalyzer): Объект анализатора с данными
        report_path (str): Путь для сохранения отчета
    """
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write("ПРЕДВАРИТЕЛЬНЫЙ ОТЧЕТ ПО АНАЛИЗУ ВАКАНСИЙ\n")
        f.write("Промышленная отрасль - данные с HH.ru\n")
        f.write(f"Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write("=" * 60 + "\n\n")
        
        # Базовая статистика
        stats = analyzer.get_basic_statistics()
        f.write("ОСНОВНЫЕ МЕТРИКИ:\n")
        f.write("-" * 40 + "\n")
        f.write(f"Всего вакансий: {stats.get('total_vacancies', 0):,}\n")
        f.write(f"Вакансий с указанной зарплатой: {stats.get('vacancies_with_salary', 0):,}\n")
        f.write(f"Уникальных работодателей: {stats.get('unique_employers', 0):,}\n")
        f.write(f"Регионов: {stats.get('unique_regions', 0):,}\n")
        
        if 'avg_salary' in stats:
            f.write(f"Средняя зарплата: {stats['avg_salary']:,.0f} руб\n")
            f.write(f"Медианная зарплата: {stats['median_salary']:,.0f} руб\n")
        f.write("\n")
        
        # Отраслевые сегменты
        segments = analyzer.analyze_industry_segments()
        if segments:
            f.write("РАСПРЕДЕЛЕНИЕ ПО ОТРАСЛЕВЫМ СЕГМЕНТАМ:\n")
            f.write("-" * 40 + "\n")
            top_segments = segments.get('top_segments', {})
            for segment, count in top_segments.items():
                percentage = segments.get('percentages', {}).get(segment, 0)
                f.write(f"{segment}: {count} вакансий ({percentage:.1f}%)\n")
            f.write("\n")
        
        # Уровни позиций
        levels = analyzer.analyze_position_levels()
        if levels:
            f.write("РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ ПОЗИЦИЙ:\n")
            f.write("-" * 40 + "\n")
            most_demanded = levels.get('most_demanded_level')
            most_count = levels.get('most_demanded_count')
            f.write(f"Наиболее востребованный уровень: {most_demanded} ({most_count} вакансий)\n\n")
            
            distribution = levels.get('distribution', {})
            for level, count in distribution.items():
                f.write(f"{level}: {count} вакансий\n")
            f.write("\n")
        
        # Сравнение зарплат
        salaries = analyzer.analyze_salaries_comparison()
        if salaries:
            f.write("СРАВНЕНИЕ ЗАРПЛАТ:\n")
            f.write("-" * 40 + "\n")
            
            engineer_vs_worker = salaries.get('engineer_vs_worker', {})
            if engineer_vs_worker:
                eng_salary = engineer_vs_worker.get('engineer_avg_salary', 0)
                worker_salary = engineer_vs_worker.get('worker_avg_salary', 0)
                ratio = engineer_vs_worker.get('salary_ratio', 0)
                
                f.write(f"Средняя зарплата инженеров: {eng_salary:,.0f} руб\n")
                f.write(f"Средняя зарплата рабочих: {worker_salary:,.0f} руб\n")
                f.write(f"Соотношение (инженер/рабочий): {ratio:.2f}\n")
            f.write("\n")
        
        # Динамика
        dynamics = analyzer.analyze_dynamics()
        if dynamics:
            f.write("ДИНАМИКА ИЗМЕНЕНИЯ СПРОСА:\n")
            f.write("-" * 40 + "\n")
            growth_rate = dynamics.get('growth_rate', 0)
            f.write(f"Темп роста за период: {growth_rate:+.1f}%\n\n")
            
            monthly_data = dynamics.get('monthly', {})
            if monthly_data:
                f.write("По месяцам:\n")
                for period, count in monthly_data.items():
                    f.write(f"  {period}: {count} вакансий\n")
            f.write("\n")

if __name__ == "__main__":
    main()