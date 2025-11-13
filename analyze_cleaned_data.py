"""
Визуализация данных из JSON файла с очищенными данными.
Адаптировано под реальную структуру данных.
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from datetime import datetime
import sys

# Настройка стилей графиков
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)

class JSONDataVisualizer:
    """
    Визуализатор для данных из JSON файла.
    """
    
    def __init__(self, json_file_path):
        """
        Инициализация с загрузкой данных из JSON.
        
        Args:
            json_file_path: Путь к JSON файлу с данными
        """
        self.df = self.load_json_data(json_file_path)
        self._fix_column_names()
        
    def load_json_data(self, file_path):
        """Загрузка данных из JSON файла."""
        print(f" Загружаем данные из: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data)
            print(f"[V] Загружено: {len(df)} вакансий, {len(df.columns)} столбцов")
            return df
            
        except Exception as e:
            print(f"[X] Ошибка загрузки: {e}")
            return pd.DataFrame()
    
    def _fix_column_names(self):
        """Исправление названий столбцов для совместимости."""
        # Покажем доступные столбцы
        print("\n ДОСТУПНЫЕ СТОЛБЦЫ:")
        for i, col in enumerate(self.df.columns[:25]):
            print(f"   {i+1:2d}. {col}")
        if len(self.df.columns) > 25:
            print(f"   ... и еще {len(self.df.columns) - 25} столбцов")
    
    def plot_industry_segments(self, save_path: str = None):
        """Визуализация распределения по отраслевым сегментам."""
        if 'industry_segment' not in self.df.columns:
            print("[X] Столбец 'industry_segment' не найден")
            return
            
        print("\n Визуализация отраслевых сегментов...")
        
        segment_counts = self.df['industry_segment'].value_counts()
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Круговая диаграмма (топ-5)
        top_segments = segment_counts.head(5)
        ax1.pie(top_segments.values, labels=top_segments.index, autopct='%1.1f%%')
        ax1.set_title('Топ-5 отраслевых сегментов', fontweight='bold')
        
        # Столбчатая диаграмма (все сегменты)
        bars = ax2.bar(range(len(segment_counts)), segment_counts.values)
        ax2.set_xticks(range(len(segment_counts)))
        ax2.set_xticklabels(segment_counts.index, rotation=45, ha='right')
        ax2.set_title('Распределение по отраслевым сегментам', fontweight='bold')
        ax2.set_ylabel('Количество вакансий')
        
        # Добавляем значения на столбцы
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    str(int(height)), ha='center', va='bottom')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
        # Выводим статистику
        print(f"\n СТАТИСТИКА СЕГМЕНТОВ:")
        for segment, count in segment_counts.head(10).items():
            percentage = (count / len(self.df)) * 100
            print(f"   {segment}: {count} вакансий ({percentage:.1f}%)")
    
    def plot_position_levels(self, save_path: str = None):
        """Визуализация распределения по уровням позиций."""
        if 'position_level' not in self.df.columns:
            print("[X] Столбец 'position_level' не найден")
            return
            
        print("\n Визуализация уровней позиций...")
        
        level_counts = self.df['position_level'].value_counts()
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        bars = ax.bar(level_counts.index, level_counts.values, 
                     color=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff99cc'])
        ax.set_title('Распределение вакансий по уровням позиций', fontweight='bold')
        ax.set_ylabel('Количество вакансий')
        ax.set_xlabel('Уровень позиции')
        
        # Добавляем значения на столбцы
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                   str(int(height)), ha='center', va='bottom')
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
        # Наиболее востребованный уровень
        most_demanded = level_counts.index[0]
        most_count = level_counts.iloc[0]
        print(f"\n Наиболее востребованный уровень: {most_demanded} ({most_count} вакансий)")
    
    def plot_salary_analysis(self, save_path: str = None):
        """Анализ и визуализация зарплат."""
        if 'salary_avg_rub' not in self.df.columns:
            print("[X] Столбец 'salary_avg_rub' не найден")
            return
            
        print("\n Анализ зарплат...")
        
        salary_data = self.df['salary_avg_rub'].dropna()
        
        if len(salary_data) == 0:
            print("[X] Нет данных о зарплатах")
            return
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Распределение зарплат
        ax1.hist(salary_data, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        ax1.axvline(salary_data.mean(), color='red', linestyle='--', linewidth=2,
                   label=f'Средняя: {salary_data.mean():,.0f} руб')
        ax1.axvline(salary_data.median(), color='green', linestyle='--', linewidth=2,
                   label=f'Медиана: {salary_data.median():,.0f} руб')
        ax1.set_title('Распределение зарплат', fontweight='bold')
        ax1.set_xlabel('Зарплата (руб)')
        ax1.set_ylabel('Количество вакансий')
        ax1.legend()
        
        # 2. Зарплаты по уровням позиций
        if 'position_level' in self.df.columns:
            salary_by_level = self.df.groupby('position_level')['salary_avg_rub'].mean().dropna()
            if len(salary_by_level) > 0:
                bars = ax2.bar(salary_by_level.index, salary_by_level.values, color='lightgreen')
                ax2.set_title('Средняя зарплата по уровням позиций', fontweight='bold')
                ax2.set_ylabel('Средняя зарплата (руб)')
                ax2.tick_params(axis='x', rotation=45)
                
                for bar, salary in zip(bars, salary_by_level.values):
                    height = bar.get_height()
                    ax2.text(bar.get_x() + bar.get_width()/2., height + 1000,
                            f'{salary:,.0f}', ha='center', va='bottom')
        
        # 3. Зарплаты по отраслевым сегментам
        if 'industry_segment' in self.df.columns:
            salary_by_segment = self.df.groupby('industry_segment')['salary_avg_rub'].mean().dropna()
            if len(salary_by_segment) > 0:
                # Сортируем по зарплате
                salary_by_segment = salary_by_segment.sort_values()
                bars = ax3.barh(salary_by_segment.index, salary_by_segment.values, color='lightcoral')
                ax3.set_title('Средняя зарплата по отраслевым сегментам', fontweight='bold')
                ax3.set_xlabel('Средняя зарплата (руб)')
                
                for bar, salary in zip(bars, salary_by_segment.values):
                    width = bar.get_width()
                    ax3.text(width + 1000, bar.get_y() + bar.get_height()/2.,
                            f'{salary:,.0f}', ha='left', va='center')
        
        # 4. Сравнение инженеров и рабочих
        if 'position_level' in self.df.columns:
            engineer_salaries = self.df[self.df['position_level'] == 'engineer']['salary_avg_rub'].dropna()
            worker_salaries = self.df[self.df['position_level'] == 'worker']['salary_avg_rub'].dropna()
            
            if len(engineer_salaries) > 0 and len(worker_salaries) > 0:
                categories = ['Инженеры', 'Рабочие']
                avg_salaries = [engineer_salaries.mean(), worker_salaries.mean()]
                counts = [len(engineer_salaries), len(worker_salaries)]
                
                bars = ax4.bar(categories, avg_salaries, alpha=0.7, color=['blue', 'orange'])
                ax4.set_title('Сравнение зарплат: Инженеры vs Рабочие', fontweight='bold')
                ax4.set_ylabel('Средняя зарплата (руб)')
                
                for bar, salary, count in zip(bars, avg_salaries, counts):
                    height = bar.get_height()
                    ax4.text(bar.get_x() + bar.get_width()/2., height + 1000,
                            f'{salary:,.0f} руб\n({count} вакансий)', 
                            ha='center', va='bottom')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
        # Статистика по зарплатам
        print(f"\n СТАТИСТИКА ЗАРПЛАТ:")
        print(f"   • Средняя зарплата: {salary_data.mean():,.0f} руб")
        print(f"   • Медианная зарплата: {salary_data.median():,.0f} руб")
        print(f"   • Минимальная зарплата: {salary_data.min():,.0f} руб")
        print(f"   • Максимальная зарплата: {salary_data.max():,.0f} руб")
        print(f"   • Вакансий с зарплатой: {len(salary_data)} ({len(salary_data)/len(self.df)*100:.1f}%)")
    
    def plot_dynamics(self, save_path: str = None):
        """Визуализация динамики публикации вакансий."""
        if 'published_at' not in self.df.columns:
            print("[X] Столбец 'published_at' не найден")
            return
            
        print("\n Анализ динамики...")
        
        # Преобразуем даты
        self.df['published_at'] = pd.to_datetime(self.df['published_at'], errors='coerce')
        date_data = self.df['published_at'].dropna()
        
        if len(date_data) == 0:
            print("[X] Нет корректных данных о датах")
            return
        
        # Группировка по месяцам
        monthly_counts = date_data.dt.to_period('M').value_counts().sort_index()
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        periods = [str(period) for period in monthly_counts.index]
        ax.plot(periods, monthly_counts.values, 'o-', linewidth=2, markersize=6, color='blue')
        ax.set_title('Динамика публикации вакансий в промышленности', fontweight='bold')
        ax.set_ylabel('Количество вакансий')
        ax.set_xlabel('Период (год-месяц)')
        
        # Добавляем значения точек
        for i, count in enumerate(monthly_counts.values):
            ax.text(i, count + 0.5, str(count), ha='center', va='bottom', fontsize=8)
        
        # Тренд
        if len(monthly_counts) > 1:
            x = np.arange(len(monthly_counts))
            z = np.polyfit(x, monthly_counts.values, 1)
            p = np.poly1d(z)
            ax.plot(periods, p(x), "r--", alpha=0.7, linewidth=2, label='Тренд')
            
            # Расчет темпа роста
            first_count = monthly_counts.iloc[0]
            last_count = monthly_counts.iloc[-1]
            growth_rate = ((last_count - first_count) / first_count) * 100 if first_count > 0 else 0
            
            ax.text(0.02, 0.98, f'Темп роста: {growth_rate:+.1f}%', 
                   transform=ax.transAxes, fontsize=12, fontweight='bold',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.8))
        
        ax.legend()
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
        print(f"\n ДИНАМИКА:")
        print(f"   • Период анализа: {periods[0]} - {periods[-1]}")
        print(f"   • Всего месяцев: {len(monthly_counts)}")
        print(f"   • Темп роста: {growth_rate:+.1f}%")
    
    def plot_top_skills(self, top_n: int = 15, save_path: str = None):
        """Визуализация наиболее востребованных навыков."""
        if 'skill_names' not in self.df.columns:
            print("[X] Столбец 'skill_names' не найден")
            return
            
        print(f"\n Анализ топ-{top_n} навыков...")
        
        # Собираем все навыки
        all_skills = []
        for skills_list in self.df['skill_names'].dropna():
            if isinstance(skills_list, list):
                all_skills.extend(skills_list)
        
        if not all_skills:
            print("[X] Нет данных о навыках")
            return
        
        skill_counts = pd.Series(all_skills).value_counts()
        top_skills = skill_counts.head(top_n)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        bars = ax.barh(top_skills.index, top_skills.values, color='lightseagreen')
        ax.set_title(f'Топ-{top_n} наиболее востребованных навыков', fontweight='bold')
        ax.set_xlabel('Количество упоминаний')
        
        # Добавляем значения
        for bar, count in zip(bars, top_skills.values):
            width = bar.get_width()
            ax.text(width + 0.5, bar.get_y() + bar.get_height()/2.,
                   str(count), ha='left', va='center', fontweight='bold')
        
        # Статистика
        total_vacancies_with_skills = self.df['skill_names'].notna().sum()
        avg_skills_per_vacancy = len(all_skills) / total_vacancies_with_skills if total_vacancies_with_skills > 0 else 0
        
        ax.text(0.02, 0.98, 
               f'Всего уникальных навыков: {len(skill_counts)}\n'
               f'Вакансий с навыками: {total_vacancies_with_skills}\n'
               f'Среднее на вакансию: {avg_skills_per_vacancy:.1f}',
               transform=ax.transAxes, fontsize=10, verticalalignment='top',
               bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.8))
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
        print(f"\n СТАТИСТИКА НАВЫКОВ:")
        print(f"   • Всего уникальных навыков: {len(skill_counts)}")
        print(f"   • Вакансий с указанными навыками: {total_vacancies_with_skills} ({total_vacancies_with_skills/len(self.df)*100:.1f}%)")
        print(f"   • Среднее количество навыков на вакансию: {avg_skills_per_vacancy:.1f}")
        print(f"\n Топ-5 навыков:")
        for i, (skill, count) in enumerate(top_skills.head(5).items(), 1):
            print(f"   {i}. {skill}: {count}")
    
    def plot_geographic_distribution(self, save_path: str = None):
        """Визуализация географического распределения вакансий."""
        area_col = 'area_name' if 'area_name' in self.df.columns else 'area.name'
        if area_col not in self.df.columns:
            print(f"[X] Столбец с регионами не найден")
            return
            
        print("\n Анализ географического распределения...")
        
        region_counts = self.df[area_col].value_counts().head(15)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Столбчатая диаграмма
        bars1 = ax1.bar(region_counts.index, region_counts.values, color='lightblue')
        ax1.set_title('Топ-15 регионов по количеству вакансий', fontweight='bold')
        ax1.set_ylabel('Количество вакансий')
        ax1.tick_params(axis='x', rotation=45)
        
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    str(int(height)), ha='center', va='bottom', fontsize=8)
        
        # Круговая диаграмма (топ-10)
        top_regions = region_counts.head(10)
        ax2.pie(top_regions.values, labels=top_regions.index, autopct='%1.1f%%')
        ax2.set_title('Доля вакансий по регионам (Топ-10)', fontweight='bold')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" График сохранен: {save_path}")
            
        plt.show()
        
        print(f"\n ГЕОГРАФИЧЕСКОЕ РАСПРЕДЕЛЕНИЕ:")
        print(f"   • Всего регионов: {self.df[area_col].nunique()}")
        print(f"   • Топ-5 регионов:")
        for i, (region, count) in enumerate(region_counts.head(5).items(), 1):
            percentage = (count / len(self.df)) * 100
            print(f"     {i}. {region}: {count} вакансий ({percentage:.1f}%)")
    
    def create_comprehensive_dashboard(self, save_path: str = "reports/dashboard.png"):
        """Создание комплексного дашборда."""
        print("\n Создание комплексного дашборда...")
        
        fig = plt.figure(figsize=(20, 15))
        
        # Сетка 3x4
        gs = fig.add_gridspec(3, 4)
        
        # 1. Основные метрики
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.axis('off')
        
        # Расчет метрик
        total_vacancies = len(self.df)
        with_salary = self.df['salary_avg_rub'].notna().sum() if 'salary_avg_rub' in self.df.columns else 0
        area_col = 'area_name' if 'area_name' in self.df.columns else 'area.name'
        unique_regions = self.df[area_col].nunique() if area_col in self.df.columns else 0
        employer_col = 'employer_name' if 'employer_name' in self.df.columns else 'employer.name'
        unique_employers = self.df[employer_col].nunique() if employer_col in self.df.columns else 0
        
        avg_salary = self.df['salary_avg_rub'].mean() if 'salary_avg_rub' in self.df.columns else 0
        median_salary = self.df['salary_avg_rub'].median() if 'salary_avg_rub' in self.df.columns else 0
        
        metrics_text = [
            "ОСНОВНЫЕ МЕТРИКИ",
            "─" * 20,
            f"Всего вакансий: {total_vacancies:,}",
            f"С зарплатой: {with_salary:,}",
            f"Регионов: {unique_regions:,}",
            f"Работодателей: {unique_employers:,}",
            f"Средняя зарплата: {avg_salary:,.0f} руб",
            f"Медианная зарплата: {median_salary:,.0f} руб"
        ]
        
        ax1.text(0.1, 0.9, '\n'.join(metrics_text), transform=ax1.transAxes,
                fontsize=11, fontfamily='monospace', verticalalignment='top',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.8))
        
        # 2. Топ сегментов
        ax2 = fig.add_subplot(gs[0, 1:3])
        if 'industry_segment' in self.df.columns:
            top_segments = self.df['industry_segment'].value_counts().head(5)
            ax2.pie(top_segments.values, labels=top_segments.index, autopct='%1.1f%%')
            ax2.set_title('Топ отраслевых сегментов', fontweight='bold')
        
        # 3. Уровни позиций
        ax3 = fig.add_subplot(gs[0, 3])
        if 'position_level' in self.df.columns:
            level_counts = self.df['position_level'].value_counts()
            ax3.bar(level_counts.index, level_counts.values, color='orange')
            ax3.set_title('Уровни позиций', fontweight='bold')
            ax3.tick_params(axis='x', rotation=45)
        
        # 4. Динамика
        ax4 = fig.add_subplot(gs[1, :2])
        if 'published_at' in self.df.columns:
            self.df['published_at'] = pd.to_datetime(self.df['published_at'], errors='coerce')
            monthly_counts = self.df['published_at'].dt.to_period('M').value_counts().sort_index()
            if len(monthly_counts) > 0:
                periods = [str(p) for p in monthly_counts.index]
                ax4.plot(periods, monthly_counts.values, 'o-')
                ax4.set_title('Динамика вакансий', fontweight='bold')
                ax4.tick_params(axis='x', rotation=45)
        
        # 5. Топ навыков
        ax5 = fig.add_subplot(gs[1, 2:])
        if 'skill_names' in self.df.columns:
            all_skills = []
            for skills_list in self.df['skill_names'].dropna():
                if isinstance(skills_list, list):
                    all_skills.extend(skills_list)
            if all_skills:
                top_skills = pd.Series(all_skills).value_counts().head(8)
                ax5.barh(top_skills.index, top_skills.values)
                ax5.set_title('Топ навыков', fontweight='bold')
        
        # 6. Регионы
        ax6 = fig.add_subplot(gs[2, :2])
        if area_col in self.df.columns:
            region_counts = self.df[area_col].value_counts().head(8)
            ax6.bar(region_counts.index, region_counts.values, color='lightgreen')
            ax6.set_title('Топ регионов', fontweight='bold')
            ax6.tick_params(axis='x', rotation=45)
        
        # 7. Зарплаты по уровням
        ax7 = fig.add_subplot(gs[2, 2:])
        if 'position_level' in self.df.columns and 'salary_avg_rub' in self.df.columns:
            salary_by_level = self.df.groupby('position_level')['salary_avg_rub'].mean().dropna()
            if len(salary_by_level) > 0:
                ax7.bar(salary_by_level.index, salary_by_level.values, color='lightcoral')
                ax7.set_title('Зарплаты по уровням', fontweight='bold')
                ax7.tick_params(axis='x', rotation=45)
        
        plt.suptitle('ДАШБОРД: АНАЛИЗ ВАКАНСИЙ ПРОМЫШЛЕННОСТИ', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f" Дашборд сохранен: {save_path}")
            
        plt.show()
    
    def create_all_visualizations(self, output_dir: str = "reports/visualizations"):
        """Создание всех визуализаций."""
        import os
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        print("=" * 60)
        print("СОЗДАНИЕ ВСЕХ ВИЗУАЛИЗАЦИЙ")
        print("=" * 60)
        
        # Создаем все графики
        self.plot_industry_segments(f"{output_dir}/1_industry_segments_{timestamp}.png")
        self.plot_position_levels(f"{output_dir}/2_position_levels_{timestamp}.png")
        self.plot_salary_analysis(f"{output_dir}/3_salary_analysis_{timestamp}.png")
        self.plot_dynamics(f"{output_dir}/4_dynamics_{timestamp}.png")
        self.plot_top_skills(save_path=f"{output_dir}/5_top_skills_{timestamp}.png")
        self.plot_geographic_distribution(f"{output_dir}/6_geographic_{timestamp}.png")
        self.create_comprehensive_dashboard(f"{output_dir}/7_dashboard_{timestamp}.png")
        
        print("\n" + "=" * 60)
        print("[V] ВСЕ ВИЗУАЛИЗАЦИИ СОЗДАНЫ!")
        print("=" * 60)
        print(f" Графики сохранены в: {output_dir}/")
        print(f" Метка времени: {timestamp}")

def main():
    """Основная функция."""
    # Путь к JSON файлу
    json_file = "data/processed/industrial_vacancies_cleaned_20251031_171113.json"
    
    if not os.path.exists(json_file):
        print(f"[X] Файл не найден: {json_file}")
        return
    
    # Создаем визуализатор
    visualizer = JSONDataVisualizer(json_file)
    
    if visualizer.df.empty:
        print("[X] Не удалось загрузить данные")
        return
    
    # Создаем все визуализации
    visualizer.create_all_visualizations()

if __name__ == "__main__":
    main()