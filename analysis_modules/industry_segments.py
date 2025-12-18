"""
МОДУЛЬ АНАЛИЗА ОТРАСЛЕВЫХ СЕГМЕНТОВ
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
    calculate_proportion_confidence_interval,
    format_proportion_confidence_interval
)


def analyze_industry_segments(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Анализирует распределение по отраслевым сегментам.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("📊 Создаем график отраслевых сегментов...")
    
    try:
        query = """
            SELECT 
                industry_segment,
                COUNT(*) as vacancy_count
            FROM vacancies 
            WHERE is_industrial = 1 
            AND industry_segment IS NOT NULL
            GROUP BY industry_segment
            ORDER BY vacancy_count DESC
            LIMIT 15
        """
        
        df = pd.read_sql_query(query, connection)
        
        # Получаем общее количество вакансий для расчета долей
        query_total = "SELECT COUNT(*) as total FROM vacancies WHERE is_industrial = 1"
        df_total = pd.read_sql_query(query_total, connection)
        total_vacancies = df_total.iloc[0]['total'] if not df_total.empty else 1
        
        # Рассчитываем долю в процентах
        df['percentage'] = (df['vacancy_count'] / total_vacancies * 100).round(1)
        
        # Вычисляем доверительные интервалы для процентных соотношений
        df['confidence_intervals'] = df.apply(
            lambda row: calculate_proportion_confidence_interval(
                int(row['vacancy_count']),
                int(total_vacancies),
                confidence_level=0.95
            ),
            axis=1
        )
        
        # Добавляем границы доверительных интервалов в DataFrame
        df['ci_lower'] = df['confidence_intervals'].apply(lambda x: x['ci_lower'])
        df['ci_upper'] = df['confidence_intervals'].apply(lambda x: x['ci_upper'])
        df['margin_of_error'] = df['confidence_intervals'].apply(lambda x: x['margin_of_error'])
        
        plt.figure(figsize=(14, 10))
        
        # Создаем горизонтальную барчарт
        bars = plt.barh(df['industry_segment'], df['vacancy_count'], 
                       color=plt.cm.Set3(np.linspace(0, 1, len(df))))
        
        plt.xlabel('Количество вакансий', fontsize=18)
        plt.ylabel('Отраслевые сегменты', fontsize=18)
        plt.title('Распределение вакансий по отраслевым сегментам', 
                 fontsize=22, fontweight='bold', pad=20)
        plt.tick_params(axis='both', labelsize=16)
        plt.gca().invert_yaxis()
        
        # Устанавливаем максимальное значение оси X до 80000
        plt.xlim(0, 80000)
        
        # Добавляем значения на бары (количество и доля) - все снаружи столбцов
        for i, (bar, count, pct) in enumerate(zip(bars, df['vacancy_count'], df['percentage'])):
            width = bar.get_width()
            plt.text(width, bar.get_y() + bar.get_height()/2, 
                    f' {count:,} ({pct}%)', ha='left', va='center', fontsize=16)
        
        plt.tight_layout()
        
        # Простое сохранение как было в оригинале
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(f'{output_dir}/01_industry_segments.png', 
                   bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()
        
        print("✅ График отраслевых сегментов создан")
        
        return {'industry_segments': df.to_dict('records')}
        
    except Exception as e:
        print(f"❌ Ошибка создания графика сегментов: {e}")
        return {}

