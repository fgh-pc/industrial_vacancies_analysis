"""
МОДУЛЬ АНАЛИЗА УРОВНЕЙ ПОЗИЦИЙ
"""

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os
from typing import Dict


def analyze_position_levels(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Анализирует распределение по уровням позиций.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("👥 Создаем график уровней позиций...")
    
    try:
        # Сначала проверяем реальное распределение (включая "другое")
        query_check = """
            SELECT 
                position_level,
                COUNT(*) as vacancy_count
            FROM vacancies 
            WHERE is_industrial = 1 
            AND position_level IS NOT NULL
            GROUP BY position_level
            ORDER BY vacancy_count DESC
        """
        df_check = pd.read_sql_query(query_check, connection)
        total_check = df_check['vacancy_count'].sum()
        df_check['percentage'] = (df_check['vacancy_count'] / total_check * 100).round(2)
        
        print(f"\n📊 РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ ПОЗИЦИЙ (всего: {total_check:,}):")
        for idx, row in df_check.iterrows():
            level = row['position_level']
            count = int(row['vacancy_count'])
            pct = row['percentage']
            print(f"   {level:<25} {count:>12,} ({pct:>6.2f}%)")
        print()
        
        # Улучшенный запрос с фильтрацией выбросов
        MIN_SALARY = 15000
        MAX_SALARY = 2000000
        
        query = """
            SELECT 
                position_level,
                COUNT(*) as vacancy_count,
                AVG(CASE 
                    WHEN has_salary = 1 
                    AND salary_avg_rub >= ? 
                    AND salary_avg_rub <= ? 
                    THEN salary_avg_rub 
                    ELSE NULL 
                END) as avg_salary,
                COUNT(CASE 
                    WHEN has_salary = 1 
                    AND salary_avg_rub >= ? 
                    AND salary_avg_rub <= ? 
                    THEN 1 
                END) as with_salary_count
            FROM vacancies 
            WHERE is_industrial = 1 
            AND position_level IS NOT NULL
            AND position_level != 'другое'
            GROUP BY position_level
            ORDER BY vacancy_count DESC
        """
        
        df = pd.read_sql_query(query, connection, params=(MIN_SALARY, MAX_SALARY, MIN_SALARY, MAX_SALARY))
        
        # Выводим детальную информацию о зарплатах
        print(f"\n💰 СРЕДНИЕ ЗАРПЛАТЫ ПО УРОВНЯМ (с фильтрацией {MIN_SALARY:,} - {MAX_SALARY:,} руб):")
        print(f"{'Уровень':<25} {'Вакансий':<12} {'С зарплатой':<12} {'Средняя зарплата':<18}")
        print('-' * 80)
        for idx, row in df.iterrows():
            level = row['position_level']
            total = int(row['vacancy_count'])
            with_salary = int(row['with_salary_count']) if pd.notna(row['with_salary_count']) else 0
            avg = int(row['avg_salary']) if pd.notna(row['avg_salary']) else 0
            pct = (with_salary / total * 100) if total > 0 else 0
            print(f"{level:<25} {total:>11,} {with_salary:>11,} ({pct:>5.1f}%) {avg:>15,} руб")
        print()
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # График 2.1: Количество вакансий
        bars1 = ax1.bar(df['position_level'], df['vacancy_count'], 
                       color='lightblue', alpha=0.7)
        ax1.set_title('Распределение по уровням позиций', fontsize=22, fontweight='bold')
        ax1.set_xlabel('Уровень позиции', fontsize=18)
        ax1.set_ylabel('Количество вакансий', fontsize=18)
        ax1.tick_params(axis='x', rotation=45, labelsize=16)
        ax1.tick_params(axis='y', labelsize=16)
        
        # Добавляем значения
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:,}', ha='center', va='bottom', fontsize=15)
        
        # График 2.2: Средние зарплаты
        bars2 = ax2.bar(df['position_level'], df['avg_salary'], 
                       color='lightcoral', alpha=0.7)
        ax2.set_title('Средние зарплаты по уровням', fontsize=22, fontweight='bold')
        ax2.set_xlabel('Уровень позиции', fontsize=18)
        ax2.set_ylabel('Средняя зарплата (руб)', fontsize=18)
        ax2.tick_params(axis='x', rotation=45, labelsize=16)
        ax2.tick_params(axis='y', labelsize=16)
        
        # Форматируем оси зарплат
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        # Добавляем значения
        for bar in bars2:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:,.0f}', ha='center', va='bottom', fontsize=15)
        
        plt.tight_layout()
        
        # Простое сохранение как было в оригинале
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.normpath(os.path.join(output_dir, '02_position_levels.png'))
        plt.savefig(output_path, 
                   bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()
        
        print("✅ График уровней позиций создан")
        
        return {'position_levels': df.to_dict('records')}
        
    except Exception as e:
        print(f"❌ Ошибка создания графика уровней: {e}")
        return {}

