"""
Модуль для визуализации результатов анализа вакансий.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Настройка стилей графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class DataVisualizer:
    """
    Класс для визуализации данных о вакансиях.
    """
    
    def __init__(self, analyzer):
        """
        Инициализация визуализатора с анализатором данных.
        
        Args:
            analyzer: Объект DataAnalyzer с загруженными данными
        """
        self.analyzer = analyzer
        self.df_vacancies = analyzer.df_vacancies
        self.df_skills = analyzer.df_skills
        
    def plot_industry_segments(self, save_path: Optional[str] = None):
        """
        Визуализация распределения вакансий по отраслевым сегментам.
        
        Args:
            save_path (str, optional): Путь для сохранения графика
        """
        segments_data = self.analyzer.analyze_industry_segments()
        
        if not segments_data:
            print("[X] Нет данных для визуализации сегментов")
            return
            
        distribution = segments_data['distribution']
        
        # Создаем фигуру с двумя субплогами
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Круговая диаграмма (только топ-5 сегментов)
        top_segments = dict(sorted(distribution.items(), 
                                 key=lambda x: x[1], reverse=True)[:5])
        other_count = sum(distribution.values()) - sum(top_segments.values())
        
        if other_count > 0:
            top_segments['другие'] = other_count
            
        ax1.pie(top_segments.values(), labels=top_segments.keys(), autopct='%1.1f%%')
        ax1.set_title('Распределение вакансий по отраслевым сегментам\n(Топ-5)', fontsize=14, fontweight='bold')
        
        # Столбчатая диаграмма (все сегменты)
        segments_df = pd.DataFrame.from_dict(distribution, orient='index', columns=['count'])
        segments_df = segments_df.sort_values('count', ascending=False)
        
        ax2.bar(range(len(segments_df)), segments_df['count'])
        ax2.set_xticks(range(len(segments_df)))
        ax2.set_xticklabels(segments_df.index, rotation=45, ha='right')
        ax2.set_title('Количество вакансий по отраслевым сегментам', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Количество вакансий')
        
        # Добавляем значения на столбцы
        for i, v in enumerate(segments_df['count']):
            ax2.text(i, v + 0.5, str(v), ha='center', va='bottom')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
    def plot_position_levels(self, save_path: Optional[str] = None):
        """
        Визуализация распределения вакансий по уровням позиций.
        
        Args:
            save_path (str, optional): Путь для сохранения графика
        """
        levels_data = self.analyzer.analyze_position_levels()
        
        if not levels_data:
            print("[X] Нет данных для визуализации уровней")
            return
            
        distribution = levels_data['distribution']
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Столбчатая диаграмма
        levels_df = pd.DataFrame.from_dict(distribution, orient='index', columns=['count'])
        levels_df = levels_df.sort_values('count', ascending=False)
        
        bars = ax.bar(levels_df.index, levels_df['count'], color='skyblue', edgecolor='navy')
        ax.set_title('Распределение вакансий по уровням позиций', fontsize=16, fontweight='bold')
        ax.set_ylabel('Количество вакансий', fontsize=12)
        ax.set_xlabel('Уровень позиции', fontsize=12)
        
        # Добавляем значения на столбцы
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                   f'{int(height)}', ha='center', va='bottom')
        
        # Выделяем наиболее востребованный уровень
        most_demanded = levels_data['most_demanded_level']
        if most_demanded:
            idx = list(levels_df.index).index(most_demanded)
            bars[idx].set_color('orange')
            ax.text(0.05, 0.95, f'Наиболее востребован: {most_demanded}', 
                   transform=ax.transAxes, fontsize=12, fontweight='bold',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
    def plot_salary_comparison(self, save_path: Optional[str] = None):
        """
        Визуализация сравнения зарплат по уровням и сегментам.
        
        Args:
            save_path (str, optional): Путь для сохранения графика
        """
        salary_data = self.analyzer.analyze_salaries_comparison()
        
        if not salary_data:
            print("[X] Нет данных для визуализации зарплат")
            return
            
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Зарплаты по уровням позиций (столбчатая диаграмма)
        by_level = salary_data.get('by_position_level', {})
        if by_level:
            levels = list(by_level.keys())
            avg_salaries = [by_level[level]['mean'] for level in levels]
            
            bars1 = ax1.bar(levels, avg_salaries, color='lightgreen', edgecolor='darkgreen')
            ax1.set_title('Средняя зарплата по уровням позиций', fontsize=14, fontweight='bold')
            ax1.set_ylabel('Зарплата (руб)', fontsize=12)
            ax1.set_xlabel('Уровень позиции', fontsize=12)
            
            # Добавляем значения на столбцы
            for bar, salary in zip(bars1, avg_salaries):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1000,
                        f'{salary:,.0f}'.replace(',', ' '), ha='center', va='bottom')
            
            ax1.tick_params(axis='x', rotation=45)
        
        # 2. Зарплаты по отраслевым сегментам (горизонтальная столбчатая)
        by_segment = salary_data.get('by_industry_segment', {})
        if by_segment:
            segments = list(by_segment.keys())
            avg_salaries = [by_segment[segment]['mean'] for segment in segments]
            
            # Сортируем по зарплате
            sorted_indices = np.argsort(avg_salaries)
            segments_sorted = [segments[i] for i in sorted_indices]
            salaries_sorted = [avg_salaries[i] for i in sorted_indices]
            
            bars2 = ax2.barh(segments_sorted, salaries_sorted, color='lightcoral', edgecolor='darkred')
            ax2.set_title('Средняя зарплата по отраслевым сегментам', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Зарплата (руб)', fontsize=12)
            
            # Добавляем значения на столбцы
            for bar, salary in zip(bars2, salaries_sorted):
                width = bar.get_width()
                ax2.text(width + 1000, bar.get_y() + bar.get_height()/2.,
                        f'{salary:,.0f}'.replace(',', ' '), ha='left', va='center')
        
        # 3. Сравнение инженеров и рабочих
        comparison = salary_data.get('engineer_vs_worker', {})
        if comparison and comparison['engineer_count'] > 0 and comparison['worker_count'] > 0:
            categories = ['Инженеры', 'Рабочие']
            salaries = [comparison['engineer_avg_salary'], comparison['worker_avg_salary']]
            counts = [comparison['engineer_count'], comparison['worker_count']]
            
            # Двойная ось Y
            ax3_secondary = ax3.twinx()
            
            # Зарплаты
            bars3 = ax3.bar(categories, salaries, alpha=0.7, color=['blue', 'red'])
            ax3.set_ylabel('Средняя зарплата (руб)', fontsize=12)
            ax3.set_title('Сравнение: Инженеры vs Рабочие', fontsize=14, fontweight='bold')
            
            # Количество вакансий (линия)
            line = ax3_secondary.plot(categories, counts, 'o-', color='green', linewidth=3, markersize=8)
            ax3_secondary.set_ylabel('Количество вакансий', fontsize=12)
            
            # Добавляем значения
            for bar, salary in zip(bars3, salaries):
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 1000,
                        f'{salary:,.0f}'.replace(',', ' '), ha='center', va='bottom')
                        
            for i, count in enumerate(counts):
                ax3_secondary.text(i, count + 5, str(count), ha='center', va='bottom', fontweight='bold')
        
        # 4. Распределение зарплат (гистограмма)
        salary_values = self.df_vacancies['salary_avg_rub'].dropna()
        if len(salary_values) > 0:
            ax4.hist(salary_values, bins=30, alpha=0.7, color='purple', edgecolor='black')
            ax4.axvline(salary_values.mean(), color='red', linestyle='--', linewidth=2, 
                       label=f'Средняя: {salary_values.mean():,.0f}'.replace(',', ' '))
            ax4.axvline(salary_values.median(), color='orange', linestyle='--', linewidth=2,
                       label=f'Медиана: {salary_values.median():,.0f}'.replace(',', ' '))
            ax4.set_title('Распределение зарплат', fontsize=14, fontweight='bold')
            ax4.set_xlabel('Зарплата (руб)', fontsize=12)
            ax4.set_ylabel('Количество вакансий', fontsize=12)
            ax4.legend()
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
    def plot_dynamics(self, save_path: Optional[str] = None):
        """
        Визуализация динамики изменения спроса на вакансии.
        
        Args:
            save_path (str, optional): Путь для сохранения графика
        """
        dynamics_data = self.analyzer.analyze_dynamics()
        
        if not dynamics_data:
            print("[X] Нет данных для визуализации динамики")
            return
            
        monthly_data = dynamics_data.get('monthly', {})
        
        if not monthly_data:
            print("[X] Нет месячных данных для визуализации")
            return
            
        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Преобразуем периоды в строки для отображения
        periods = [str(period) for period in monthly_data.keys()]
        counts = list(monthly_data.values())
        
        # Линейный график динамики
        line = ax.plot(periods, counts, 'o-', linewidth=3, markersize=8, color='blue', 
                      markerfacecolor='red', markeredgecolor='darkred')
        
        ax.set_title('Динамика спроса на вакансии в промышленности', fontsize=16, fontweight='bold')
        ax.set_ylabel('Количество вакансий', fontsize=12)
        ax.set_xlabel('Период (год-месяц)', fontsize=12)
        
        # Добавляем значения точек
        for i, count in enumerate(counts):
            ax.text(i, count + 0.5, str(count), ha='center', va='bottom', fontweight='bold')
        
        # Вычисляем и отображаем тренд
        if len(counts) > 1:
            x = np.arange(len(counts))
            z = np.polyfit(x, counts, 1)
            p = np.poly1d(z)
            ax.plot(periods, p(x), "r--", alpha=0.7, linewidth=2, label='Тренд')
            
            # Отображаем темп роста
            growth_rate = dynamics_data.get('growth_rate', 0)
            growth_color = 'green' if growth_rate > 0 else 'red'
            ax.text(0.02, 0.98, f'Темп роста: {growth_rate:+.1f}%', 
                   transform=ax.transAxes, fontsize=12, fontweight='bold',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor=growth_color, alpha=0.3))
        
        ax.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        


# Упрощенная функция для быстрой визуализации
def create_quick_visualizations(analyzer, save_dir: str = "reports/"):
    """
    Создание всех основных визуализаций одной функцией.
    
    Args:
        analyzer: Объект DataAnalyzer
        save_dir (str): Директория для сохранения графиков
    """
    import os
    os.makedirs(save_dir, exist_ok=True)
    visualizer = DataVisualizer(analyzer)
    
    print(" Создаем визуализации...")
    
    # Создаем все графики
    visualizer.plot_industry_segments(f"{save_dir}industry_segments.png")
    visualizer.plot_position_levels(f"{save_dir}position_levels.png")
    visualizer.plot_salary_comparison(f"{save_dir}salary_comparison.png")
    visualizer.plot_dynamics(f"{save_dir}dynamics.png")
    visualizer.plot_top_skills(save_path=f"{save_dir}top_skills.png")
    visualizer.create_comprehensive_dashboard(f"{save_dir}dashboard.png")
    
    print("[V] Все визуализации созданы и сохранены!")