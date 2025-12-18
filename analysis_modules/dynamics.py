"""
МОДУЛЬ АНАЛИЗА ДИНАМИКИ
"""

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import os
from typing import Dict
from datetime import datetime, timedelta


def analyze_dynamics(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Анализирует динамику спроса по полумесяцам.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("📈 Создаем график динамики спроса...")
    
    try:
        MIN_SALARY = 20000
        MAX_SALARY = 1000000
        
        query = """
            SELECT 
                strftime('%Y-%m', published_at) || '-' || 
                CASE WHEN CAST(strftime('%d', published_at) AS INTEGER) <= 15 THEN '01' ELSE '15' END as period,
                COUNT(*) as vacancy_count,
                salary_avg_rub
            FROM vacancies 
            WHERE is_industrial = 1 
            AND published_at IS NOT NULL
            AND published_at >= '2025-10-01'
            AND published_at < '2025-12-01'
            GROUP BY period
            HAVING vacancy_count >= 10
            ORDER BY period
        """
        
        df_raw = pd.read_sql_query(query, connection)
        
        # Рассчитываем медианные зарплаты по полумесяцам
        df_salary = pd.read_sql_query("""
            SELECT 
                strftime('%Y-%m', published_at) || '-' || 
                CASE WHEN CAST(strftime('%d', published_at) AS INTEGER) <= 15 THEN '01' ELSE '15' END as period,
                salary_avg_rub
            FROM vacancies 
            WHERE is_industrial = 1 
            AND published_at IS NOT NULL
            AND published_at >= '2025-10-01'
            AND published_at < '2025-12-01'
            AND has_salary = 1
            AND salary_avg_rub BETWEEN ? AND ?
        """, connection, params=(MIN_SALARY, MAX_SALARY))
        
        # Группируем и считаем медиану
        salary_by_period = df_salary.groupby('period')['salary_avg_rub'].median().to_dict()
        
        df = df_raw.copy()
        df['avg_salary'] = df['period'].map(salary_by_period).fillna(0)
        df = df.sort_values('period')
        
        if len(df) > 1:
            # Преобразуем периоды в даты
            def period_to_date(period_str):
                year_month, day = period_str.rsplit('-', 1)
                year, month = year_month.split('-')
                return datetime(int(year), int(month), int(day))
            
            # Получаем первый и последний период из данных
            first_period = df['period'].iloc[0]
            last_period = df['period'].iloc[-1]
            
            first_date = period_to_date(first_period)
            last_date = period_to_date(last_period)
            
            # Генерируем все полумесячные периоды между первым и последним
            all_periods = []
            
            # Начинаем с первого периода
            current_date = first_date
            # Нормализуем первую дату к началу полумесяца (01 или 15)
            if current_date.day <= 15:
                current_date = current_date.replace(day=1)
            else:
                current_date = current_date.replace(day=15)
            
            # Генерируем все периоды до последнего включительно
            while True:
                # Формируем строку периода
                period_str = current_date.strftime('%Y-%m') + '-' + ('01' if current_date.day == 1 else '15')
                
                # Добавляем период, если он не превышает последний
                period_date = period_to_date(period_str)
                if period_date <= last_date:
                    all_periods.append(period_str)
                
                # Если достигли последнего периода, останавливаемся
                if period_str == last_period or period_date >= last_date:
                    break
                
                # Переходим к следующему полумесяцу
                if current_date.day == 1:
                    current_date = current_date.replace(day=15)
                else:
                    # Переходим к следующему месяцу
                    if current_date.month == 12:
                        current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
                    else:
                        current_date = current_date.replace(month=current_date.month + 1, day=1)
            
            # Создаем словарь для сопоставления периодов с индексами
            period_to_index = {period: idx for idx, period in enumerate(all_periods)}
            
            # Создаем массивы для построения графика
            x_indices = [period_to_index[p] for p in df['period']]
            y_values = df['vacancy_count'].values
            
            fig, ax = plt.subplots(1, 1, figsize=(14, 8))
            
            # График: Динамика количества вакансий
            ax.plot(x_indices, y_values, 
                    marker='o', linewidth=2, markersize=6, color='#2E8B57')
            ax.set_title('Динамика количества вакансий по полумесяцам', 
                         fontsize=18, fontweight='bold', pad=20)
            ax.set_ylabel('Количество вакансий', fontsize=16)
            ax.set_xlabel('Период', fontsize=16)
            ax.tick_params(axis='both', labelsize=14)
            ax.grid(True, alpha=0.3)
            
            # Настраиваем ось Y: от 0 до 70000 с интервалом 10000
            ax.set_ylim(0, 70000)
            ax.set_yticks(range(0, 70001, 10000))
            
            # Устанавливаем все периоды на оси X
            ax.set_xticks(range(len(all_periods)))
            ax.set_xticklabels(all_periods, rotation=45, ha='right', fontsize=15)
            
            # Добавляем значения на точки
            for idx, count in zip(x_indices, y_values):
                ax.annotate(f'{count:,}', (idx, count), 
                           textcoords="offset points", xytext=(0,10), 
                           ha='center', fontsize=15)
            
            plt.tight_layout()
            
            # Простое сохранение как было в оригинале
            os.makedirs(output_dir, exist_ok=True)
            plt.savefig(f'{output_dir}/04_dynamics.png', 
                       bbox_inches='tight', dpi=300, facecolor='white')
            plt.close()
            
            # Расчет роста
            first_count = df.iloc[0]['vacancy_count']
            last_count = df.iloc[-1]['vacancy_count']
            growth_rate = ((last_count - first_count) / first_count) * 100
            
            result = {
                'dynamics': {
                    'monthly_data': df.to_dict('records'),
                    'growth_rate': round(growth_rate, 2),
                    'periods_analyzed': len(df)
                }
            }
            
            print("✅ График динамики создан")
            return result
        
        return {}
        
    except Exception as e:
        print(f"❌ Ошибка создания графика динамики: {e}")
        import traceback
        traceback.print_exc()
        return {}


def create_custom_dynamics_chart(connection: sqlite3.Connection, output_dir: str, 
                                 output_filename: str = "04_dynamics_custom.png") -> bool:
    """
    Создает график динамики с измененными значениями:
    - 10,582 → 30,282
    - 56,735 → 56,735 (без изменений)
    - 98,362 → 68,362
    - 35,975 → 45,975
    """
    print("📈 Создаем график динамики с кастомными значениями...")
    
    try:
        query = """
            SELECT 
                strftime('%Y-%m', published_at) || '-' || 
                CASE WHEN CAST(strftime('%d', published_at) AS INTEGER) <= 15 THEN '1' ELSE '2' END as period,
                COUNT(*) as vacancy_count
            FROM vacancies 
            WHERE is_industrial = 1 
            AND published_at IS NOT NULL
            AND published_at >= date('now', '-45 days')
            GROUP BY period
            HAVING vacancy_count >= 10
            ORDER BY period
        """
        
        df = pd.read_sql_query(query, connection)
        df = df.sort_values('period')
        
        if len(df) > 0:
            # Словарь для замены значений
            value_replacements = {
                10582: 30282,   # 10,582 → 30,282
                56735: 56735,   # 56,735 → 56,735 (без изменений)
                98362: 68362,   # 98,362 → 68,362
                35975: 45975    # 35,975 → 45,975
            }
            
            print(f"\nИсходные данные из базы:")
            for period, count in zip(df['period'], df['vacancy_count']):
                print(f"   {period}: {count:,}")
            
            # Применяем замены
            replacements_made = []
            def replace_value(val):
                val_int = int(val)
                if val_int in value_replacements:
                    new_val = value_replacements[val_int]
                    if val_int != new_val:
                        replacements_made.append((val_int, new_val))
                    return new_val
                # Проверяем близкие значения (допуск в 50)
                for old_val, new_val in value_replacements.items():
                    if abs(val_int - old_val) <= 50:
                        if val_int != new_val:
                            replacements_made.append((val_int, new_val))
                        return new_val
                return val
            
            df['vacancy_count'] = df['vacancy_count'].apply(replace_value)
            
            if replacements_made:
                print(f"\nВыполнены замены:")
                for old_val, new_val in replacements_made:
                    print(f"   {old_val:,} → {new_val:,}")
            
            # Создаем график
            fig, ax = plt.subplots(1, 1, figsize=(14, 8))
            
            ax.plot(df['period'], df['vacancy_count'], 
                    marker='o', linewidth=2, markersize=6, color='#2E8B57')
            ax.set_title('Динамика количества вакансий по полумесяцам', 
                         fontsize=22, fontweight='bold', pad=20)
            ax.set_ylabel('Количество вакансий', fontsize=18)
            ax.set_xlabel('Период', fontsize=18)
            ax.tick_params(axis='both', labelsize=16)
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            for i, (period, count) in enumerate(zip(df['period'], df['vacancy_count'])):
                ax.annotate(f'{count:,}', (period, count), 
                           textcoords="offset points", xytext=(0,10), 
                           ha='center', fontsize=15)
            
            plt.tight_layout()
            
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, output_filename)
            plt.savefig(output_path, bbox_inches='tight', dpi=300, facecolor='white')
            plt.close()
            
            print(f"\n✅ График сохранен: {output_path}")
            print(f"   Финальные значения в графике:")
            for period, count in zip(df['period'], df['vacancy_count']):
                print(f"   {period}: {count:,}")
            
            return True
        else:
            print("❌ Нет данных для создания графика")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка создания графика: {e}")
        import traceback
        traceback.print_exc()
        return False
