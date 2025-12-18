"""
МОДУЛЬ АНАЛИЗА РЕГИОНАЛЬНОГО РАСПРЕДЕЛЕНИЯ
"""

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os
from typing import Dict


def analyze_regional_distribution(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Анализирует региональное распределение.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("🌍 Создаем график регионального распределения...")
    
    try:
        MIN_SALARY = 20000
        MAX_SALARY = 1000000
        
        # Сначала получаем количество вакансий по регионам
        query_count = """
            SELECT 
                region,
                COUNT(*) as vacancy_count
            FROM vacancies 
            WHERE is_industrial = 1 
            AND region IS NOT NULL
            AND region != ''
            GROUP BY region
            HAVING vacancy_count >= 50
            ORDER BY vacancy_count DESC
            LIMIT 15
        """
        
        df_count = pd.read_sql_query(query_count, connection)
        
        # Затем получаем медианные зарплаты для этих регионов
        if not df_count.empty:
            regions_list = df_count['region'].tolist()
            placeholders = ','.join(['?' for _ in regions_list])
            
            query_salary = f"""
                SELECT 
                    region,
                    salary_avg_rub
                FROM vacancies 
                WHERE is_industrial = 1 
                AND region IN ({placeholders})
                AND has_salary = 1
                AND salary_avg_rub BETWEEN ? AND ?
            """
            
            params = regions_list + [MIN_SALARY, MAX_SALARY]
            df_salary_raw = pd.read_sql_query(query_salary, connection, params=params)
            
            # Рассчитываем средние зарплаты (более точные значения, чем медиана)
            # Используем среднее арифметическое для более реалистичных значений
            salary_means = df_salary_raw.groupby('region')['salary_avg_rub'].mean().to_dict()
            
            # Объединяем данные
            df = df_count.copy()
            df['avg_salary'] = df['region'].map(salary_means).fillna(0)
            df = df.sort_values('vacancy_count', ascending=False)
        else:
            df = pd.DataFrame(columns=['region', 'vacancy_count', 'avg_salary'])
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
        
        # График 5.1: Количество вакансий по регионам
        bars1 = ax1.barh(df['region'], df['vacancy_count'], color='lightseagreen')
        ax1.set_title('Топ-15 регионов по количеству вакансий', fontsize=22, fontweight='bold')
        ax1.set_xlabel('Количество вакансий', fontsize=18)
        ax1.set_ylabel('Регионы', fontsize=18)
        ax1.tick_params(axis='both', labelsize=16)
        ax1.invert_yaxis()
        
        for bar in bars1:
            width = bar.get_width()
            ax1.text(width, bar.get_y() + bar.get_height()/2, 
                    f' {width:,}', ha='left', va='center', fontsize=15)
        
        # График 5.2: Зарплаты по регионам
        # Заменяем нулевые и NaN значения на 0 для корректного отображения
        df['avg_salary'] = df['avg_salary'].fillna(0).replace([None], 0)
        
        bars2 = ax2.barh(df['region'], df['avg_salary'], color='coral')
        ax2.set_title('Средние зарплаты по регионам', fontsize=22, fontweight='bold')
        ax2.set_xlabel('Средняя зарплата (руб)', fontsize=18)
        ax2.set_ylabel('Регионы', fontsize=18)
        ax2.tick_params(axis='both', labelsize=16)
        ax2.invert_yaxis()
        # Форматируем ось X с разделителями тысяч, но без округления
        ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}' if x > 0 else '0'))
        
        for bar in bars2:
            width = bar.get_width()
            if width > 0:  # Отображаем только валидные зарплаты
                # Показываем точное значение без округления до тысяч
                ax2.text(width, bar.get_y() + bar.get_height()/2, 
                        f' {int(width):,}', ha='left', va='center', fontsize=15)
        
        plt.tight_layout()
        
        # Убеждаемся, что директория существует
        os.makedirs(output_dir, exist_ok=True)
        
        # Нормализуем путь для корректной работы в Windows
        output_file = os.path.normpath(os.path.join(output_dir, '05_regional_distribution.png'))
        
        plt.savefig(output_file, 
                   bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()
        
        print("✅ График регионального распределения создан")
        
        return {'regional_distribution': df.to_dict('records')}
        
    except Exception as e:
        print(f"❌ Ошибка создания графика регионов: {e}")
        return {}

