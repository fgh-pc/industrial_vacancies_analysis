"""
МОДУЛЬ СОХРАНЕНИЯ ТЕКСТОВОГО ОТЧЕТА
"""

from datetime import datetime
from typing import Dict


def save_text_report(report_data: Dict, output_dir: str, db_path: str):
    """
    Сохраняет текстовый отчет.
    
    Args:
        report_data: Словарь с данными для отчета
        output_dir: Директория для сохранения отчета
        db_path: Путь к базе данных
    """
    print("💾 Сохраняем текстовый отчет...")
    
    try:
        report_file = f'{output_dir}/comprehensive_analysis_report.txt'
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("КОМПЛЕКСНЫЙ АНАЛИЗ ПРОМЫШЛЕННЫХ ВАКАНСИЙ РОССИИ\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"База данных: {db_path}\n\n")
            
            # Основные метрики
            metrics = report_data.get('summary_metrics', {})
            f.write("ОСНОВНЫЕ МЕТРИКИ:\n")
            f.write("-" * 50 + "\n")
            f.write(f"• Всего промышленных вакансий: {metrics.get('total_vacancies', 0):,}\n")
            f.write(f"• Охват зарплатами: {metrics.get('salary_coverage', 0)}%\n")
            f.write(f"• Средняя зарплата: {metrics.get('avg_salary', 0):,} руб\n")
            f.write(f"• Уникальных работодателей: {metrics.get('unique_employers', 0):,}\n")
            f.write(f"• Регионов: {metrics.get('unique_regions', 0)}\n\n")
            
            # Отраслевые сегменты
            segments = report_data.get('industry_segments', [])
            total = metrics.get('total_vacancies', 1)
            f.write("ТОП ОТРАСЛЕВЫХ СЕГМЕНТОВ:\n")
            f.write("-" * 50 + "\n")
            for i, segment in enumerate(segments[:10], 1):
                count = segment.get('vacancy_count', 0)
                # Используем процент из данных, если есть, иначе вычисляем
                pct = segment.get('percentage')
                if pct is None:
                    pct = (count / total * 100) if total > 0 else 0
                f.write(f"{i:2d}. {segment['industry_segment']}: {count:,} вакансий ({pct:.1f}%)\n")
            f.write("\n")
            
            # Уровни позиций
            levels = report_data.get('position_levels', [])
            f.write("РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ ПОЗИЦИЙ:\n")
            f.write("-" * 50 + "\n")
            for level in levels:
                f.write(f"• {level['position_level']}: {level['vacancy_count']:,} вакансий, {level['avg_salary']:,.0f} руб\n")
            f.write("\n")
            
            # Сравнение зарплат
            salaries = report_data.get('salary_comparison', [])
            f.write("СРАВНЕНИЕ ЗАРПЛАТ:\n")
            f.write("-" * 50 + "\n")
            for salary in salaries:
                avg = salary.get('avg_salary', 0)
                median = salary.get('median_salary', 0)
                category = salary.get('category', '').replace('\n', ' ')
                f.write(f"• {category}:\n")
                f.write(f"  - Средняя зарплата: {avg:,.0f} руб\n")
                f.write(f"  - Медианная зарплата: {median:,.0f} руб\n")
                
                # Добавляем информацию о доверительных интервалах
                ci = salary.get('confidence_interval', {})
                if ci and ci.get('n', 0) > 0:
                    f.write(f"  - 95% Доверительный интервал: [{ci.get('ci_lower', 0):,.0f}, {ci.get('ci_upper', 0):,.0f}] руб\n")
                    f.write(f"  - Стандартная ошибка среднего: {ci.get('sem', 0):,.0f} руб\n")
                    f.write(f"  - Маржа ошибки: ±{ci.get('margin_of_error', 0):,.0f} руб\n")
                    f.write(f"  - Размер выборки: {ci.get('n', 0):,}\n")
            f.write("\n")
            
            # Динамика
            dynamics = report_data.get('dynamics', {})
            if dynamics:
                f.write("ДИНАМИКА СПРОСА:\n")
                f.write("-" * 50 + "\n")
                f.write(f"• Проанализировано периодов: {dynamics.get('periods_analyzed', 0)}\n")
                f.write(f"• Изменение спроса: {dynamics.get('growth_rate', 0):+.1f}%\n\n")
            
            # Регионы
            regions = report_data.get('regional_distribution', [])
            f.write("ТОП РЕГИОНОВ:\n")
            f.write("-" * 50 + "\n")
            for i, region in enumerate(regions[:5], 1):
                f.write(f"{i}. {region['region']}: {region['vacancy_count']:,} вакансий, {region['avg_salary']:,.0f} руб\n")
            f.write("\n")
            
            # Навыки
            skills = report_data.get('top_skills', [])
            f.write("ТОП НАВЫКОВ:\n")
            f.write("-" * 50 + "\n")
            for i, skill in enumerate(skills[:10], 1):
                f.write(f"{i:2d}. {skill['skill_name']}: {skill['frequency']} упоминаний\n")
            f.write("\n")
            
            # Прогноз
            forecast = report_data.get('forecast', {})
            if forecast:
                f.write("ПРОГНОЗ НА СЛЕДУЮЩИЙ ГОД:\n")
                f.write("-" * 50 + "\n")
                f.write(f"• Тренд: {forecast.get('trend_slope', 0):.1f} вакансий/месяц\n")
                f.write(f"• Надежность прогноза (R²): {forecast.get('r_squared', 0):.3f}\n\n")
            
            f.write("СОЗДАННЫЕ ГРАФИКИ:\n")
            f.write("-" * 50 + "\n")
            charts = [
                "01_industry_segments.png - Распределение по отраслям",
                "02_position_levels.png - Уровни позиций и зарплаты",
                "03_salary_comparison.png - Сравнение зарплат",
                "04_dynamics.png - Динамика спроса",
                "05_regional_distribution.png - Региональное распределение",
                "06_skills_analysis.png - Востребованные навыки",
                "07_forecast.png - Прогноз спроса",
                "08_summary_dashboard.png - Сводный дашборд"
            ]
            for chart in charts:
                f.write(f"• {chart}\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("АНАЛИЗ ЗАВЕРШЕН\n")
            f.write("=" * 80 + "\n")
        
        print(f"✅ Текстовый отчет сохранен: {report_file}")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения отчета: {e}")

