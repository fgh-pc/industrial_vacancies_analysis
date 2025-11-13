"""
ВИЗУАЛИЗАТОР ДАННЫХ ДЛЯ ПРОМЫШЛЕННЫХ ВАКАНСИЙ
Базовые визуализации для анализа данных
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import numpy as np
from typing import Dict, List, Optional
import logging

class IndustrialDataVisualizer:
    """
    Визуализатор для промышленных вакансий.
    Создает графики и диаграммы для анализа данных.
    """
    
    def __init__(self, analyzer):
        self.analyzer = analyzer
        self.df_vacancies = self._load_vacancies_data()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования."""
        logger = logging.getLogger('IndustrialDataVisualizer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _load_vacancies_data(self):
        """Загружает данные вакансий из базы данных."""
        try:
            query = "SELECT * FROM vacancies WHERE is_industrial = 1 LIMIT 50000"
            return pd.read_sql_query(query, self.analyzer.connection)
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка загрузки данных для визуализации: {e}")
            return pd.DataFrame()

    def create_all_visualizations(self):
        """Создает все визуализации для отчета."""
        try:
            self.logger.info("🎨 Создаем визуализации...")
            
            # Проверяем есть ли данные
            if self.df_vacancies.empty:
                self.logger.warning("⚠️ Нет данных для визуализации")
                return
            
            # Создаем директорию для графиков
            os.makedirs("reports/charts", exist_ok=True)
            
            # Создаем базовые графики
            self._create_industry_segment_chart()
            self._create_salary_distribution_chart()
            self._create_regional_distribution_chart()
            self._create_position_level_chart()
            self._create_experience_chart()
            self._create_skills_chart()
            
            self.logger.info("✅ Базовые визуализации созданы")
            
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка при создании визуализаций: {e}")

    def _create_industry_segment_chart(self):
        """Создает график распределения по отраслевым сегментам."""
        try:
            if 'industry_segment' in self.df_vacancies.columns:
                segment_counts = self.df_vacancies['industry_segment'].value_counts()
                
                # Создаем pie chart
                plt.figure(figsize=(12, 8))
                
                # Берем топ-10 сегментов, остальные объединяем в "Другие"
                top_segments = segment_counts.head(9)
                other_count = segment_counts[9:].sum()
                
                if other_count > 0:
                    top_segments['Другие'] = other_count
                
                colors = plt.cm.Set3(np.linspace(0, 1, len(top_segments)))
                
                plt.pie(top_segments.values, labels=top_segments.index, autopct='%1.1f%%',
                       colors=colors, startangle=90)
                plt.title('Распределение вакансий по отраслевым сегментам', fontsize=14, fontweight='bold')
                
                # Сохраняем график
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                plt.savefig(f'reports/charts/industry_segments_{timestamp}.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                
                self.logger.info(f"📊 Создан график сегментов: {len(top_segments)} категорий")
                
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика сегментов: {e}")

    def _create_salary_distribution_chart(self):
        """Создает график распределения зарплат."""
        try:
            if 'salary_avg_rub' in self.df_vacancies.columns:
                salaries = self.df_vacancies['salary_avg_rub'].dropna()
                
                if len(salaries) > 0:
                    plt.figure(figsize=(12, 6))
                    
                    # Гистограмма зарплат
                    plt.subplot(1, 2, 1)
                    plt.hist(salaries, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
                    plt.xlabel('Зарплата (руб)')
                    plt.ylabel('Количество вакансий')
                    plt.title('Распределение зарплат')
                    plt.grid(True, alpha=0.3)
                    
                    # Box plot зарплат
                    plt.subplot(1, 2, 2)
                    plt.boxplot(salaries, vert=False)
                    plt.xlabel('Зарплата (руб)')
                    plt.title('Box plot зарплат')
                    plt.grid(True, alpha=0.3)
                    
                    plt.tight_layout()
                    
                    # Сохраняем график
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    plt.savefig(f'reports/charts/salary_distribution_{timestamp}.png', 
                               bbox_inches='tight', dpi=300)
                    plt.close()
                    
                    self.logger.info(f"💰 Создан график зарплат: {len(salaries)} вакансий с зарплатами")
                    
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика зарплат: {e}")

    def _create_regional_distribution_chart(self):
        """Создает график регионального распределения."""
        try:
            if 'region' in self.df_vacancies.columns:
                region_counts = self.df_vacancies['region'].value_counts().head(15)
                
                plt.figure(figsize=(14, 8))
                
                # Horizontal bar chart для лучшей читаемости
                bars = plt.barh(region_counts.index, region_counts.values, 
                               color='lightcoral', alpha=0.7)
                
                plt.xlabel('Количество вакансий')
                plt.title('Топ-15 регионов по количеству вакансий', fontsize=14, fontweight='bold')
                plt.gca().invert_yaxis()  # Чтобы самый большой был сверху
                
                # Добавляем значения на бары
                for bar in bars:
                    width = bar.get_width()
                    plt.text(width, bar.get_y() + bar.get_height()/2, 
                            f' {int(width)}', ha='left', va='center')
                
                plt.grid(True, alpha=0.3, axis='x')
                plt.tight_layout()
                
                # Сохраняем график
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                plt.savefig(f'reports/charts/regional_distribution_{timestamp}.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                
                self.logger.info(f"🌍 Создан график регионов: {len(region_counts)} регионов")
                
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика регионов: {e}")

    def _create_position_level_chart(self):
        """Создает график распределения по уровням позиций."""
        try:
            if 'position_level' in self.df_vacancies.columns:
                level_counts = self.df_vacancies['position_level'].value_counts()
                
                plt.figure(figsize=(10, 6))
                
                colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#c2c2f0']
                
                plt.pie(level_counts.values, labels=level_counts.index, autopct='%1.1f%%',
                       colors=colors[:len(level_counts)], startangle=90)
                plt.title('Распределение по уровням позиций', fontsize=14, fontweight='bold')
                
                # Сохраняем график
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                plt.savefig(f'reports/charts/position_levels_{timestamp}.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                
                self.logger.info(f"👥 Создан график уровней позиций: {len(level_counts)} уровней")
                
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика уровней: {e}")

    def _create_experience_chart(self):
        """Создает график распределения по опыту работы."""
        try:
            if 'experience' in self.df_vacancies.columns:
                experience_counts = self.df_vacancies['experience'].value_counts()
                
                plt.figure(figsize=(12, 6))
                
                bars = plt.bar(experience_counts.index, experience_counts.values,
                              color='lightgreen', alpha=0.7)
                
                plt.xlabel('Требуемый опыт')
                plt.ylabel('Количество вакансий')
                plt.title('Распределение по требуемому опыту работы', fontsize=14, fontweight='bold')
                plt.xticks(rotation=45, ha='right')
                
                # Добавляем значения на бары
                for bar in bars:
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width()/2., height,
                            f'{int(height)}', ha='center', va='bottom')
                
                plt.grid(True, alpha=0.3, axis='y')
                plt.tight_layout()
                
                # Сохраняем график
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                plt.savefig(f'reports/charts/experience_distribution_{timestamp}.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                
                self.logger.info(f"📚 Создан график опыта: {len(experience_counts)} категорий")
                
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика опыта: {e}")

    def _create_skills_chart(self):
        """Создает график топ навыков."""
        try:
            # Загружаем данные навыков из базы
            skills_query = """
                SELECT 
                    skill_name,
                    COUNT(*) as frequency
                FROM skills s
                JOIN vacancies v ON s.vacancy_id = v.id
                WHERE v.is_industrial = 1
                GROUP BY skill_name
                ORDER BY frequency DESC
                LIMIT 20
            """
            
            df_skills = pd.read_sql_query(skills_query, self.analyzer.connection)
            
            if not df_skills.empty:
                plt.figure(figsize=(14, 10))
                
                # Horizontal bar chart для навыков
                bars = plt.barh(df_skills['skill_name'], df_skills['frequency'],
                               color='orange', alpha=0.7)
                
                plt.xlabel('Частота упоминания')
                plt.title('Топ-20 наиболее востребованных навыков', fontsize=14, fontweight='bold')
                plt.gca().invert_yaxis()
                
                # Добавляем значения на бары
                for bar in bars:
                    width = bar.get_width()
                    plt.text(width, bar.get_y() + bar.get_height()/2, 
                            f' {int(width)}', ha='left', va='center')
                
                plt.grid(True, alpha=0.3, axis='x')
                plt.tight_layout()
                
                # Сохраняем график
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                plt.savefig(f'reports/charts/top_skills_{timestamp}.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                
                self.logger.info(f"🔧 Создан график навыков: {len(df_skills)} навыков")
                
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика навыков: {e}")

    def create_comparison_charts(self):
        """Создает сравнительные графики."""
        try:
            self.logger.info("📈 Создаем сравнительные графики...")
            
            if self.df_vacancies.empty:
                return
            
            # Сравнение зарплат по отраслевым сегментам
            self._create_salary_by_segment_chart()
            
            # Сравнение зарплат по уровням позиций
            self._create_salary_by_level_chart()
            
            # Сравнение по регионам
            self._create_regional_salary_chart()
            
            self.logger.info("✅ Сравнительные графики созданы")
            
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания сравнительных графиков: {e}")

    def _create_salary_by_segment_chart(self):
        """Создает график сравнения зарплат по отраслевым сегментам."""
        try:
            if all(col in self.df_vacancies.columns for col in ['industry_segment', 'salary_avg_rub']):
                # Берем только сегменты с достаточным количеством данных
                segment_salaries = self.df_vacancies.groupby('industry_segment')['salary_avg_rub'].agg([
                    'mean', 'count'
                ]).query('count >= 10').sort_values('mean', ascending=False).head(10)
                
                if len(segment_salaries) > 0:
                    plt.figure(figsize=(12, 8))
                    
                    bars = plt.bar(segment_salaries.index, segment_salaries['mean'],
                                  color='lightblue', alpha=0.7)
                    
                    plt.xlabel('Отраслевой сегмент')
                    plt.ylabel('Средняя зарплата (руб)')
                    plt.title('Средняя зарплата по отраслевым сегментам', fontsize=14, fontweight='bold')
                    plt.xticks(rotation=45, ha='right')
                    
                    # Форматируем подписи осей
                    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
                    
                    plt.grid(True, alpha=0.3, axis='y')
                    plt.tight_layout()
                    
                    # Сохраняем график
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    plt.savefig(f'reports/charts/salary_by_segment_{timestamp}.png', 
                               bbox_inches='tight', dpi=300)
                    plt.close()
                    
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика зарплат по сегментам: {e}")

    def _create_salary_by_level_chart(self):
        """Создает график сравнения зарплат по уровням позиций."""
        try:
            if all(col in self.df_vacancies.columns for col in ['position_level', 'salary_avg_rub']):
                level_salaries = self.df_vacancies.groupby('position_level')['salary_avg_rub'].mean().sort_values(ascending=False)
                
                if len(level_salaries) > 0:
                    plt.figure(figsize=(10, 6))
                    
                    colors = ['gold', 'silver', 'peru', 'lightblue', 'lightcoral']
                    bars = plt.bar(level_salaries.index, level_salaries.values,
                                  color=colors[:len(level_salaries)], alpha=0.7)
                    
                    plt.xlabel('Уровень позиции')
                    plt.ylabel('Средняя зарплата (руб)')
                    plt.title('Средняя зарплата по уровням позиций', fontsize=14, fontweight='bold')
                    
                    # Форматируем подписи осей
                    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
                    
                    plt.grid(True, alpha=0.3, axis='y')
                    plt.tight_layout()
                    
                    # Сохраняем график
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    plt.savefig(f'reports/charts/salary_by_level_{timestamp}.png', 
                               bbox_inches='tight', dpi=300)
                    plt.close()
                    
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика зарплат по уровням: {e}")

    def _create_regional_salary_chart(self):
        """Создает график сравнения зарплат по регионам."""
        try:
            if all(col in self.df_vacancies.columns for col in ['region', 'salary_avg_rub']):
                # Берем регионы с достаточным количеством данных
                regional_salaries = self.df_vacancies.groupby('region')['salary_avg_rub'].agg([
                    'mean', 'count'
                ]).query('count >= 10').sort_values('mean', ascending=False).head(15)
                
                if len(regional_salaries) > 0:
                    plt.figure(figsize=(14, 8))
                    
                    bars = plt.barh(regional_salaries.index, regional_salaries['mean'],
                                   color='lightseagreen', alpha=0.7)
                    
                    plt.xlabel('Средняя зарплата (руб)')
                    plt.title('Топ-15 регионов по средней зарплате', fontsize=14, fontweight='bold')
                    plt.gca().invert_yaxis()
                    
                    # Форматируем подписи осей
                    plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
                    
                    plt.grid(True, alpha=0.3, axis='x')
                    plt.tight_layout()
                    
                    # Сохраняем график
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    plt.savefig(f'reports/charts/regional_salaries_{timestamp}.png', 
                               bbox_inches='tight', dpi=300)
                    plt.close()
                    
        except Exception as e:
            self.logger.error(f"⚠️ Ошибка создания графика зарплат по регионам: {e}")


# Функция для быстрого создания всех визуализаций
def create_industrial_visualizations(analyzer):
    """
    Быстрое создание всех визуализаций для промышленных вакансий.
    """
    visualizer = IndustrialDataVisualizer(analyzer)
    visualizer.create_all_visualizations()
    visualizer.create_comparison_charts()
    print("✅ Все визуализации созданы и сохранены в папке reports/charts/")


if __name__ == "__main__":
    # Пример использования
    from analysis.data_analyzer import IndustrialDataAnalyzer
    
    analyzer = IndustrialDataAnalyzer()
    if analyzer.connect_to_database():
        create_industrial_visualizations(analyzer)
        analyzer.close_connection()