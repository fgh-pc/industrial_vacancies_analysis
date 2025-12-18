"""
МОДУЛЬ СРАВНЕНИЯ ЗАРПЛАТ
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import os
from typing import Dict
import sys

# Добавляем путь к корню проекта для импорта модулей
project_root = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, project_root)
from src.statistics.error_estimation import (
    calculate_confidence_interval,
    calculate_statistical_summary,
    format_confidence_interval
)


def analyze_salary_comparison(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Анализирует сравнение средних и медианных зарплат по категориям.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("💰 Создаем график сравнения зарплат...")
    
    try:
        # Сначала выводим реальные минимальные и максимальные значения с фильтрацией выбросов
        MIN_REALISTIC = 15000  # Минимальная разумная зарплата
        MAX_REALISTIC = 2000000  # Максимальная разумная зарплата
        
        query_range = """
            SELECT 
                MIN(salary_avg_rub) as min_salary,
                MAX(salary_avg_rub) as max_salary,
                AVG(salary_avg_rub) as avg_salary,
                COUNT(*) as total
            FROM vacancies 
            WHERE is_industrial = 1 
            AND has_salary = 1 
            AND salary_avg_rub IS NOT NULL
            AND salary_avg_rub >= ? 
            AND salary_avg_rub <= ?
        """
        df_range = pd.read_sql_query(query_range, connection, params=(MIN_REALISTIC, MAX_REALISTIC))
        if not df_range.empty and df_range.iloc[0]['total'] > 0:
            min_salary = int(df_range.iloc[0]['min_salary'])
            max_salary = int(df_range.iloc[0]['max_salary'])
            avg_salary = int(df_range.iloc[0]['avg_salary'])
            total = int(df_range.iloc[0]['total'])
            print(f"\n📊 Диапазон зарплат (с фильтрацией выбросов):")
            print(f"   Минимальная: {min_salary:,} руб")
            print(f"   Максимальная: {max_salary:,} руб")
            print(f"   Средняя: {avg_salary:,} руб")
            print(f"   Всего вакансий с зарплатой (15,000 - 2,000,000 руб): {total:,}\n")
        
        MIN_SALARY = 20000
        MAX_SALARY = 1000000
        
        # Определяем категории специалистов (без "Все промышленные вакансии")
        categories = {
            'high_qualified': "Высококвалифицированные\n(инженеры, руководители)",
            'medium_qualified': "Среднеквалифицированные\n(рабочие, специалисты)"
        }
        
        salary_data = []
        
        for category, description in categories.items():
            if category == 'high_qualified':
                query = """
                    SELECT salary_avg_rub
                    FROM vacancies 
                    WHERE is_industrial = 1 AND has_salary = 1
                    AND salary_avg_rub >= ? AND salary_avg_rub <= ?
                    AND (position_level IN ('инженер', 'руководитель', 'высшее_руководство')
                         OR name LIKE '%инженер%' OR name LIKE '%руководитель%')
                """
            else:  # medium_qualified
                query = """
                    SELECT salary_avg_rub
                    FROM vacancies 
                    WHERE is_industrial = 1 AND has_salary = 1
                    AND salary_avg_rub >= ? AND salary_avg_rub <= ?
                    AND (position_level IN ('рабочий', 'специалист')
                         OR name LIKE '%рабочий%' OR name LIKE '%сварщик%' 
                         OR name LIKE '%токарь%' OR name LIKE '%электрик%')
                """
            
            df = pd.read_sql_query(query, connection, params=(MIN_SALARY, MAX_SALARY))
            
            # Убираем NULL и NaN значения для точного расчета
            df_clean = df['salary_avg_rub'].dropna()
            df_clean = df_clean[df_clean > 0]  # Убираем нули и отрицательные значения
            
            if len(df_clean) > 0:
                # Вычисляем среднюю и медианную зарплату только на валидных данных
                avg_salary = df_clean.mean()
                median_salary = df_clean.median()
                
                # Вычисляем доверительный интервал для средней зарплаты
                ci = calculate_confidence_interval(df_clean, confidence_level=0.95)
                
                # Полная статистическая сводка
                stats_summary = calculate_statistical_summary(df_clean, confidence_level=0.95)
            else:
                avg_salary = 0
                median_salary = 0
                ci = {
                    'mean': 0.0,
                    'ci_lower': 0.0,
                    'ci_upper': 0.0,
                    'sem': 0.0,
                    'margin_of_error': 0.0,
                    'n': 0
                }
                stats_summary = {
                    'n': 0,
                    'mean': 0.0,
                    'median': 0.0,
                    'std': 0.0,
                    'sem': 0.0,
                    'confidence_interval': ci
                }
            
            salary_data.append({
                'category': description,
                'avg_salary': float(avg_salary) if not pd.isna(avg_salary) else 0,
                'median_salary': float(median_salary) if not pd.isna(median_salary) else 0,
                'confidence_interval': {
                    'ci_lower': float(ci['ci_lower']),
                    'ci_upper': float(ci['ci_upper']),
                    'margin_of_error': float(ci['margin_of_error']),
                    'sem': float(ci['sem']),
                    'n': int(ci['n'])
                },
                'statistical_summary': stats_summary
            })
            
            # Выводим информацию о погрешности
            if ci['n'] > 0:
                print(f"   {description}:")
                print(f"      Средняя: {avg_salary:,.0f} руб")
                print(f"      95% ДИ: [{ci['ci_lower']:,.0f}, {ci['ci_upper']:,.0f}] руб")
                print(f"      Стандартная ошибка: {ci['sem']:,.0f} руб")
                print(f"      Маржа ошибки: ±{ci['margin_of_error']:,.0f} руб")
                print(f"      Размер выборки: {ci['n']:,}")
        
        df_salaries = pd.DataFrame(salary_data)
        
        # Создаем график с двумя столбцами: средняя и медианная зарплата
        fig, ax = plt.subplots(figsize=(14, 8))
        
        x = np.arange(len(df_salaries))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, df_salaries['avg_salary'], width, 
                      label='Средняя зарплата', color='#2E8B57', alpha=0.7)
        bars2 = ax.bar(x + width/2, df_salaries['median_salary'], width, 
                      label='Медианная зарплата', color='#FFA500', alpha=0.7)
        
        ax.set_ylabel('Зарплата (руб)', fontsize=18)
        ax.set_title('Сравнение средней и медианной зарплаты по категориям специалистов', 
                    fontsize=22, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(df_salaries['category'], fontsize=17)
        ax.legend(fontsize=16, loc='upper right', bbox_to_anchor=(1.02, 1.0))
        ax.tick_params(axis='both', labelsize=16)
        
        # Форматируем оси
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        # Добавляем значения на бары
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height + 1000,
                           f'{height:,.0f}', ha='center', va='bottom', 
                           fontweight='bold', fontsize=16)
        
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        # Простое сохранение как было в оригинале
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(f'{output_dir}/03_salary_comparison.png', 
                   bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()
        
        print("✅ График сравнения зарплат создан")
        
        return {'salary_comparison': salary_data}
        
    except Exception as e:
        print(f"❌ Ошибка создания графика зарплат: {e}")
        import traceback
        traceback.print_exc()
        return {}
