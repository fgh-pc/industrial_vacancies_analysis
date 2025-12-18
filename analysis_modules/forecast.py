"""
МОДУЛЬ ПРОГНОЗИРОВАНИЯ
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import os
from datetime import datetime, timedelta
from scipy import stats
from typing import Dict


def analyze_forecast(connection: sqlite3.Connection, output_dir: str) -> Dict:
    """
    Создает прогноз спроса на 3 месяца вперед.
    
    Args:
        connection: Соединение с базой данных
        output_dir: Директория для сохранения результатов
        
    Returns:
        Словарь с данными для отчета
    """
    print("🔮 Создаем график прогноза...")
    
    try:
        # Получаем исторические данные по полмесяцам (используем ту же логику и даты, что и в dynamics.py)
        query = """
            SELECT 
                strftime('%Y-%m', published_at) || '-' || 
                CASE WHEN CAST(strftime('%d', published_at) AS INTEGER) <= 15 THEN '01' ELSE '15' END as period,
                COUNT(*) as vacancy_count
            FROM vacancies 
            WHERE is_industrial = 1 
            AND published_at IS NOT NULL
            AND published_at >= '2025-10-01'
            AND published_at < '2025-12-01'
            GROUP BY period
            HAVING vacancy_count >= 10
            ORDER BY period
        """
        
        df_history = pd.read_sql_query(query, connection)
        
        if len(df_history) < 4:
            print("⚠️  Недостаточно данных для прогноза (нужно минимум 4 полмесяца)")
            return {}
        
        # Простой прогноз на основе линейного тренда
        x = np.arange(len(df_history))
        y = df_history['vacancy_count'].values
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        
        # Базовое значение - среднее последних 4 полмесяцев для реалистичности
        base_value = y[-4:].mean() if len(y) >= 4 else y.mean()
        
        # Прогноз на 2 месяца (4 полмесяца) вперед
        future_periods_count = 4
        forecast_periods = []
        forecast_values = []
        
        # Получаем последний период из истории для продолжения нумерации
        last_period_str = df_history.iloc[-1]['period']
        # Формат: "YYYY-MM-01" или "YYYY-MM-15"
        parts = last_period_str.split('-')
        last_year = int(parts[0])
        last_month = int(parts[1])
        last_day = int(parts[2])  # 01 или 15
        
        # Получаем статистику для прогноза
        historical_mean = df_history['vacancy_count'].mean()
        historical_std = df_history['vacancy_count'].std() if len(df_history) > 1 else historical_mean * 0.1
        last_value = y[-1]  # Последнее значение из истории
        
        # Вычисляем среднюю скорость изменения (тренд) за последние периоды
        if len(y) >= 3:
            recent_slope = (y[-1] - y[-3]) / 2  # Изменение за последние 2 периода
        elif len(y) >= 2:
            recent_slope = y[-1] - y[-2]
        else:
            recent_slope = 0
        
        print(f"   Исторические данные: последнее значение = {last_value:.0f}, тренд = {slope:.2f}, std = {historical_std:.2f}")
        
        # Инициализируем переменные для расчета периодов
        current_month = last_month
        current_year = last_year
        current_day = last_day
        
        # Переходим к следующему периоду после последнего исторического
        if current_day == 1:
            current_day = 15
        else:  # current_day == 15
            current_day = 1
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        for i in range(1, future_periods_count + 1):
            # Правильный индекс для будущего периода
            future_x = len(df_history) - 1 + i
            
            # Базовое значение прогноза по линейному тренду
            trend_value = intercept + slope * future_x
            
            # Используем комбинацию линейного тренда и локального тренда
            # Локальный тренд более важен для ближайших периодов
            if len(y) >= 3:
                local_trend = (y[-1] - y[-3]) / 2  # Локальный тренд за последние 2 периода
            else:
                local_trend = slope
            
            # Затухание: чем дальше, тем больше используем линейный тренд, меньше локальный
            local_weight = max(0.2, 1.0 - (i - 1) * 0.2)  # От 100% до 20% локального тренда
            trend_weight = 1.0 - local_weight
            
            # Базовый прогноз: комбинация линейного и локального тренда
            linear_prediction = trend_value
            local_prediction = last_value + local_trend * i
            predicted = linear_prediction * trend_weight + local_prediction * local_weight
            
            # Добавляем небольшую вариацию на основе исторического разброса
            if historical_std > 0:
                # Вариация уменьшается со временем (прогноз становится менее точным)
                variation_magnitude = historical_std * 0.2 * max(0.3, 1.0 - (i - 1) * 0.2)
                # Используем синусоиду для плавной вариации между периодами
                phase = i * 2 * np.pi / (future_periods_count * 0.8)
                variation = variation_magnitude * np.sin(phase)
                predicted = predicted + variation
            
            # Ограничения: не ниже 50% и не выше 150% от последнего значения
            min_value = max(0, last_value * 0.5)
            max_value = last_value * 1.5
            predicted = max(min_value, min(max_value, int(predicted)))
            
            # Сохраняем прогнозное значение
            forecast_values.append(int(predicted))
            forecast_periods.append(f"{current_year}-{current_month:02d}-{current_day:02d}")
            
            # Переходим к следующему периоду для следующей итерации
            if current_day == 1:
                current_day = 15
            else:  # current_day == 15
                current_day = 1
                current_month += 1
                if current_month > 12:
                    current_month = 1
                    current_year += 1
        
        # Создаем DataFrame для прогноза
        df_forecast = pd.DataFrame({
            'period': forecast_periods,
            'vacancy_count': forecast_values,
            'type': 'прогноз'
        })
        
        # Подготовка исторических данных
        df_history = df_history[['period', 'vacancy_count']].copy()
        df_history['type'] = 'история'
        
        # Объединяем данные
        df_combined = pd.concat([df_history, df_forecast], ignore_index=True)
        
        plt.figure(figsize=(14, 8))
        
        # Используем только прогнозные данные для отображения
        forecast_data = df_forecast
        
        # График только прогноза (без истории)
        forecast_x = range(len(forecast_data))
        plt.plot(forecast_x, forecast_data['vacancy_count'].values, 
                marker='s', linestyle='--', linewidth=2, label='Прогноз', color='#FF6347', markersize=6)
        
        # Добавляем значения над точками (как в dynamics.py)
        for i, (period, count) in enumerate(zip(forecast_data['period'], forecast_data['vacancy_count'])):
            plt.annotate(f'{count:,}', (i, count), 
                       textcoords="offset points", xytext=(0,10), 
                       ha='center', fontsize=15)
        
        # Формируем подписи для оси X - только периоды прогноза
        forecast_periods = list(forecast_data['period'].values)
        x_ticks = list(range(len(forecast_data)))
        x_labels = forecast_periods  # Показываем все метки прогноза
        
        plt.xticks(x_ticks, x_labels, rotation=45, ha='right', fontsize=15)
        plt.yticks(fontsize=16)
        
        plt.title('Прогноз спроса на промышленных специалистов на 2 месяца', 
                 fontsize=22, fontweight='bold', pad=20)
        plt.ylabel('Количество вакансий', fontsize=18)
        plt.xlabel('Период (полмесяца)', fontsize=18)
        plt.legend(fontsize=17)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Простое сохранение
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(f'{output_dir}/07_forecast.png', 
                   bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()
        
        # Сохраняем данные прогноза
        result = {
            'forecast': {
                'historical_data': df_history[['period', 'vacancy_count']].to_dict('records'),
                'forecast_data': forecast_data[['period', 'vacancy_count']].to_dict('records'),
                'trend_slope': round(slope, 2),
                'r_squared': round(r_value**2, 3),
                'base_value': round(base_value, 0)
            }
        }
        
        print("✅ График прогноза создан")
        return result
        
    except Exception as e:
        print(f"❌ Ошибка создания графика прогноза: {e}")
        import traceback
        traceback.print_exc()
        return {}
