"""
МОДУЛЬ СОЗДАНИЯ СВОДНОГО ДАШБОРДА
"""

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os
from typing import Dict


def analyze_dashboard(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Создает сводный дашборд с ключевыми метриками.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("📋 Создаем сводный дашборд...")
    
    try:
        # Собираем ключевые метрики
        metrics = {}
        
        # Общее количество вакансий
        query_total = "SELECT COUNT(*) as total FROM vacancies WHERE is_industrial = 1"
        df_total = pd.read_sql_query(query_total, connection)
        metrics['total_vacancies'] = int(df_total.iloc[0]['total'])
        
        # Вакансии с зарплатой
        query_salary = "SELECT COUNT(*) as total FROM vacancies WHERE is_industrial = 1 AND has_salary = 1"
        df_salary = pd.read_sql_query(query_salary, connection)
        metrics['with_salary'] = int(df_salary.iloc[0]['total'])
        metrics['salary_coverage'] = round((metrics['with_salary'] / metrics['total_vacancies']) * 100, 1)
        
        # Уникальные работодатели
        query_employers = "SELECT COUNT(DISTINCT employer_name) as total FROM vacancies WHERE is_industrial = 1"
        df_employers = pd.read_sql_query(query_employers, connection)
        metrics['unique_employers'] = int(df_employers.iloc[0]['total'])
        
        # Регионы
        query_regions = "SELECT COUNT(DISTINCT region) as total FROM vacancies WHERE is_industrial = 1"
        df_regions = pd.read_sql_query(query_regions, connection)
        metrics['unique_regions'] = int(df_regions.iloc[0]['total'])
        
        # Средняя зарплата
        query_avg_salary = "SELECT AVG(salary_avg_rub) as avg FROM vacancies WHERE is_industrial = 1 AND has_salary = 1"
        df_avg_salary = pd.read_sql_query(query_avg_salary, connection)
        metrics['avg_salary'] = int(df_avg_salary.iloc[0]['avg'] or 0)
        
        # Создаем дашборд
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('СВОДНЫЙ ДАШБОРД: АНАЛИЗ ПРОМЫШЛЕННЫХ ВАКАНСИЙ', 
                    fontsize=24, fontweight='bold', y=0.95)
        
        # Метрика 1: Общее количество
        axes[0,0].text(0.5, 0.5, f"{metrics['total_vacancies']:,}", 
                      ha='center', va='center', fontsize=36, fontweight='bold', color='#2E8B57')
        axes[0,0].set_title('Всего промышленных вакансий', fontsize=20, fontweight='bold')
        axes[0,0].axis('off')
        
        # Метрика 2: Охват зарплатами
        axes[0,1].text(0.5, 0.5, f"{metrics['salary_coverage']}%", 
                      ha='center', va='center', fontsize=36, fontweight='bold', color='#FF6347')
        axes[0,1].set_title('Охват зарплатами', fontsize=20, fontweight='bold')
        axes[0,1].axis('off')
        
        # Метрика 3: Средняя зарплата
        axes[1,0].text(0.5, 0.5, f"{metrics['avg_salary']:,} руб", 
                      ha='center', va='center', fontsize=30, fontweight='bold', color='#1E90FF')
        axes[1,0].set_title('Средняя зарплата', fontsize=20, fontweight='bold')
        axes[1,0].axis('off')
        
        # Метрика 4: Работодатели и регионы
        text = f"Работодатели: {metrics['unique_employers']:,}\nРегионы: {metrics['unique_regions']}"
        axes[1,1].text(0.5, 0.5, text, ha='center', va='center', 
                      fontsize=24, fontweight='bold', color='#FF8C00')
        axes[1,1].set_title('География и работодатели', fontsize=20, fontweight='bold')
        axes[1,1].axis('off')
        
        plt.tight_layout()
        
        # Убеждаемся, что директория существует
        os.makedirs(output_dir, exist_ok=True)
        
        # Нормализуем путь для корректной работы в Windows
        output_file = os.path.normpath(os.path.join(output_dir, '08_summary_dashboard.png'))
        
        plt.savefig(output_file, 
                   bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()
        
        print("✅ Сводный дашборд создан")
        
        return {'summary_metrics': metrics}
        
    except Exception as e:
        print(f"❌ Ошибка создания дашборда: {e}")
        return {}

