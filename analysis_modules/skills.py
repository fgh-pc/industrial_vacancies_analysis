"""
МОДУЛЬ АНАЛИЗА НАВЫКОВ
"""

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os
from typing import Dict


def analyze_skills(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Анализирует востребованные навыки.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("🔧 Создаем график анализа навыков...")
    
    try:
        query = """
            SELECT 
                skill_name,
                COUNT(*) as frequency,
                COUNT(DISTINCT vacancy_id) as unique_vacancies
            FROM skills s
            JOIN vacancies v ON s.vacancy_id = v.id
            WHERE v.is_industrial = 1
            GROUP BY skill_name
            ORDER BY frequency DESC
            LIMIT 20
        """
        
        df = pd.read_sql_query(query, connection)
        
        if not df.empty:
            plt.figure(figsize=(14, 10))
            
            bars = plt.barh(df['skill_name'], df['frequency'], color='goldenrod')
            
            plt.xlabel('Частота упоминания', fontsize=18)
            plt.ylabel('Навыки', fontsize=18)
            plt.title('Топ-20 наиболее востребованных навыков в промышленности', 
                     fontsize=22, fontweight='bold', pad=20)
            plt.tick_params(axis='both', labelsize=16)
            plt.gca().invert_yaxis()
            
            for bar in bars:
                width = bar.get_width()
                plt.text(width, bar.get_y() + bar.get_height()/2, 
                        f' {width}', ha='left', va='center', fontsize=15)
            
            plt.tight_layout()
            
            # Убеждаемся, что директория существует
            os.makedirs(output_dir, exist_ok=True)
            
            # Нормализуем путь для корректной работы в Windows
            output_file = os.path.normpath(os.path.join(output_dir, '06_skills_analysis.png'))
            
            plt.savefig(output_file, 
                       bbox_inches='tight', dpi=300, facecolor='white')
            plt.close()
            
            print("✅ График анализа навыков создан")
            
            return {'top_skills': df.to_dict('records')}
        
        return {}
        
    except Exception as e:
        print(f"❌ Ошибка создания графика навыков: {e}")
        return {}

