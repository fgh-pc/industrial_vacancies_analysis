"""
УТИЛИТА ДЛЯ ОБЪЕДИНЕНИЯ JSON ФАЙЛОВ И ГЕНЕРАЦИИ ОТЧЕТА
Объединяет несколько файлов с вакансиями, удаляет дубликаты и создает отчет
"""

import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Set
import glob
import matplotlib.pyplot as plt
import seaborn as sns

class VacancyMerger:
    """
    Класс для объединения JSON файлов с вакансиями и генерации отчетов.
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.all_vacancies = []
        self.stats = {
            'total_files_processed': 0,
            'total_vacancies_before': 0,
            'total_vacancies_after': 0,
            'duplicates_removed': 0,
            'date_range': {'min': None, 'max': None},
            'regions_count': 0,
            'industries_count': 0,
            'professional_roles_count': 0,
            'salary_stats': {},
            'collection_methods': {}
        }
        
    def find_json_files(self) -> List[str]:
        """
        Находит все JSON файлы с вакансиями в директории data.
        
        Returns:
            Список путей к JSON файлам
        """
        pattern = os.path.join(self.data_dir, "*.json")
        json_files = glob.glob(pattern)
        
        # Исключаем файлы статистики и уже объединенные файлы
        excluded_keywords = ['stats', 'report', 'merged', 'final', 'duplicates']
        filtered_files = [
            f for f in json_files 
            if not any(keyword in f.lower() for keyword in excluded_keywords)
        ]
        
        print(f"📁 Найдено JSON файлов: {len(filtered_files)}")
        return filtered_files
    
    def load_and_merge_files(self, json_files: List[str]) -> List[Dict]:
        """
        Загружает и объединяет данные из всех JSON файлов.
        
        Args:
            json_files: Список путей к JSON файлам
            
        Returns:
            Объединенный список вакансий
        """
        all_vacancies = []
        
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    all_vacancies.extend(data)
                    self.stats['total_files_processed'] += 1
                    print(f"✅ Загружено {len(data)} вакансий из {os.path.basename(file_path)}")
                else:
                    print(f"⚠️ Файл {file_path} не содержит список вакансий")
                    
            except Exception as e:
                print(f"❌ Ошибка загрузки {file_path}: {e}")
        
        self.stats['total_vacancies_before'] = len(all_vacancies)
        print(f"📊 Всего вакансий до объединения: {len(all_vacancies):,}")
        
        return all_vacancies
    
    def remove_duplicates(self, vacancies: List[Dict]) -> List[Dict]:
        """
        Удаляет дубликаты вакансий по ID.
        
        Args:
            vacancies: Список вакансий
            
        Returns:
            Список уникальных вакансий
        """
        seen_ids = set()
        unique_vacancies = []
        
        for vacancy in vacancies:
            vacancy_id = vacancy.get('id')
            if vacancy_id and vacancy_id not in seen_ids:
                seen_ids.add(vacancy_id)
                unique_vacancies.append(vacancy)
        
        duplicates_removed = len(vacancies) - len(unique_vacancies)
        self.stats['duplicates_removed'] = duplicates_removed
        self.stats['total_vacancies_after'] = len(unique_vacancies)
        
        print(f"🔄 Удалено дубликатов: {duplicates_removed:,}")
        print(f"📊 Уникальных вакансий: {len(unique_vacancies):,}")
        
        return unique_vacancies
    
    def analyze_vacancies(self, vacancies: List[Dict]):
        """
        Анализирует вакансии и собирает статистику.
        
        Args:
            vacancies: Список вакансий для анализа
        """
        if not vacancies:
            return
            
        # Анализ дат
        dates = []
        for vacancy in vacancies:
            published_at = vacancy.get('published_at')
            if published_at:
                try:
                    date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    dates.append(date)
                except:
                    continue
        
        if dates:
            self.stats['date_range']['min'] = min(dates)
            self.stats['date_range']['max'] = max(dates)
        
        # Анализ регионов
        regions = set()
        for vacancy in vacancies:
            region = vacancy.get('collection_region') or vacancy.get('area', {}).get('name')
            if region:
                regions.add(region)
        self.stats['regions_count'] = len(regions)
        
        # Анализ зарплат
        salaries = []
        for vacancy in vacancies:
            salary = vacancy.get('salary')
            if salary:
                salary_from = salary.get('from')
                salary_to = salary.get('to')
                if salary_from:
                    salaries.append(salary_from)
                if salary_to:
                    salaries.append(salary_to)
        
        if salaries:
            self.stats['salary_stats'] = {
                'min': min(salaries),
                'max': max(salaries),
                'avg': sum(salaries) / len(salaries),
                'median': sorted(salaries)[len(salaries) // 2],
                'count': len(salaries)
            }
        
        # Анализ методов сбора
        collection_methods = {}
        for vacancy in vacancies:
            method = vacancy.get('collection_method', 'unknown')
            collection_methods[method] = collection_methods.get(method, 0) + 1
        self.stats['collection_methods'] = collection_methods
        
        # Анализ отраслей и ролей
        industries = set()
        professional_roles = set()
        
        for vacancy in vacancies:
            industry_id = vacancy.get('industry_id')
            if industry_id:
                industries.add(industry_id)
            
            role_id = vacancy.get('role_id')
            if role_id:
                professional_roles.add(role_id)
        
        self.stats['industries_count'] = len(industries)
        self.stats['professional_roles_count'] = len(professional_roles)
    
    def generate_report(self, output_file: str = "merged_vacancies_report.md"):
        """
        Генерирует подробный отчет в формате Markdown.
        
        Args:
            output_file: Имя файла для отчета
        """
        report = []
        
        # Заголовок отчета
        report.append("# 📊 ОТЧЕТ ПО ОБЪЕДИНЕННЫМ ПРОМЫШЛЕННЫМ ВАКАНСИЯМ")
        report.append(f"**Дата генерации:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Основная статистика
        report.append("## 📈 ОСНОВНАЯ СТАТИСТИКА")
        report.append("")
        report.append(f"- **Обработано файлов:** {self.stats['total_files_processed']}")
        report.append(f"- **Вакансий до объединения:** {self.stats['total_vacancies_before']:,}")
        report.append(f"- **Вакансий после объединения:** {self.stats['total_vacancies_after']:,}")
        report.append(f"- **Дубликатов удалено:** {self.stats['duplicates_removed']:,}")
        report.append(f"- **Эффективность очистки:** {(self.stats['duplicates_removed'] / self.stats['total_vacancies_before'] * 100):.1f}%")
        report.append("")
        
        # Период сбора
        if self.stats['date_range']['min'] and self.stats['date_range']['max']:
            date_range = self.stats['date_range']
            days_diff = (date_range['max'] - date_range['min']).days
            report.append("## 📅 ПЕРИОД СБОРА ДАННЫХ")
            report.append("")
            report.append(f"- **Начало периода:** {date_range['min'].strftime('%Y-%m-%d')}")
            report.append(f"- **Конец периода:** {date_range['max'].strftime('%Y-%m-%d')}")
            report.append(f"- **Продолжительность:** {days_diff} дней")
            report.append("")
        
        # Географическое распределение
        report.append("## 🗺️ ГЕОГРАФИЧЕСКОЕ РАСПРЕДЕЛЕНИЕ")
        report.append("")
        report.append(f"- **Регионов охвачено:** {self.stats['regions_count']}")
        report.append("")
        
        # Анализ зарплат
        salary_stats = self.stats['salary_stats']
        if salary_stats:
            report.append("## 💰 АНАЛИЗ ЗАРПЛАТ")
            report.append("")
            report.append(f"- **Вакансий с указанной зарплатой:** {salary_stats['count']:,}")
            report.append(f"- **Минимальная зарплата:** {salary_stats['min']:,.0f} руб")
            report.append(f"- **Максимальная зарплата:** {salary_stats['max']:,.0f} руб")
            report.append(f"- **Средняя зарплата:** {salary_stats['avg']:,.0f} руб")
            report.append(f"- **Медианная зарплата:** {salary_stats['median']:,.0f} руб")
            report.append("")
        
        # Методы сбора
        report.append("## 🔧 МЕТОДЫ СБОРА ДАННЫХ")
        report.append("")
        for method, count in self.stats['collection_methods'].items():
            percentage = (count / self.stats['total_vacancies_after']) * 100
            report.append(f"- **{method}:** {count:,} вакансий ({percentage:.1f}%)")
        report.append("")
        
        # Классификация данных
        report.append("## 🏭 КЛАССИФИКАЦИЯ ДАННЫХ")
        report.append("")
        report.append(f"- **Промышленных отраслей:** {self.stats['industries_count']}")
        report.append(f"- **Профессиональных ролей:** {self.stats['professional_roles_count']}")
        report.append("")
        
        # Рекомендации
        report.append("## 💡 РЕКОМЕНДАЦИИ И ВЫВОДЫ")
        report.append("")
        
        if self.stats['duplicates_removed'] > 0:
            report.append(f"✅ **Эффективная очистка:** Удалено {self.stats['duplicates_removed']:,} дубликатов")
        
        if self.stats['regions_count'] > 50:
            report.append("✅ **Широкий географический охват:** Данные из множества регионов")
        else:
            report.append("⚠️ **Ограниченный охват:** Рассмотрите расширение сбора данных")
        
        if salary_stats and salary_stats['count'] / self.stats['total_vacancies_after'] > 0.5:
            report.append("✅ **Хорошая заполненность зарплат:** Большинство вакансий содержат информацию о зарплате")
        else:
            report.append("⚠️ **Недостаток данных о зарплатах:** Многие вакансии не содержат информацию о зарплате")
        
        # Сохраняем отчет
        report_path = os.path.join(self.data_dir, output_file)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print(f"📄 Отчет сохранен: {report_path}")
        
        return report_path
    
    def create_visualizations(self, vacancies: List[Dict]):
        """
        Создает визуализации для отчета.
        
        Args:
            vacancies: Список вакансий для визуализации
        """
        try:
            # Создаем папку для графиков
            plots_dir = os.path.join(self.data_dir, "plots")
            os.makedirs(plots_dir, exist_ok=True)
            
            # Подготовка данных
            df_data = []
            for vacancy in vacancies:
                row = {
                    'id': vacancy.get('id'),
                    'published_at': vacancy.get('published_at'),
                    'region': vacancy.get('collection_region') or vacancy.get('area', {}).get('name'),
                    'salary_from': vacancy.get('salary', {}).get('from'),
                    'salary_to': vacancy.get('salary', {}).get('to'),
                    'collection_method': vacancy.get('collection_method', 'unknown')
                }
                df_data.append(row)
            
            df = pd.DataFrame(df_data)
            
            # 1. Распределение по месяцам
            if 'published_at' in df.columns and not df['published_at'].isna().all():
                df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
                df['month'] = df['published_at'].dt.to_period('M')
                
                monthly_counts = df['month'].value_counts().sort_index()
                
                plt.figure(figsize=(12, 6))
                monthly_counts.plot(kind='bar', color='skyblue')
                plt.title('Распределение вакансий по месяцам')
                plt.xlabel('Месяц')
                plt.ylabel('Количество вакансий')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(os.path.join(plots_dir, 'monthly_distribution.png'), dpi=300, bbox_inches='tight')
                plt.close()
            
            # 2. Топ-10 регионов
            if 'region' in df.columns:
                top_regions = df['region'].value_counts().head(10)
                
                plt.figure(figsize=(10, 6))
                top_regions.plot(kind='barh', color='lightgreen')
                plt.title('Топ-10 регионов по количеству вакансий')
                plt.xlabel('Количество вакансий')
                plt.tight_layout()
                plt.savefig(os.path.join(plots_dir, 'top_regions.png'), dpi=300, bbox_inches='tight')
                plt.close()
            
            # 3. Методы сбора
            if 'collection_method' in df.columns:
                method_counts = df['collection_method'].value_counts()
                
                plt.figure(figsize=(8, 8))
                plt.pie(method_counts.values, labels=method_counts.index, autopct='%1.1f%%')
                plt.title('Распределение по методам сбора')
                plt.savefig(os.path.join(plots_dir, 'collection_methods.png'), dpi=300, bbox_inches='tight')
                plt.close()
                
            print(f"📊 Визуализации сохранены в: {plots_dir}")
            
        except Exception as e:
            print(f"⚠️ Ошибка при создании визуализаций: {e}")
    
    def merge_and_analyze(self, output_filename: str = "merged_industrial_vacancies.json"):
        """
        Основной метод для объединения файлов и генерации отчета.
        
        Args:
            output_filename: Имя выходного файла
            
        Returns:
            Путь к объединенному файлу
        """
        print("=" * 60)
        print("🔄 НАЧАЛО ОБЪЕДИНЕНИЯ JSON ФАЙЛОВ")
        print("=" * 60)
        
        # Находим файлы
        json_files = self.find_json_files()
        if not json_files:
            print("❌ Не найдено JSON файлов для обработки")
            return None
        
        # Загружаем и объединяем
        all_vacancies = self.load_and_merge_files(json_files)
        if not all_vacancies:
            print("❌ Не удалось загрузить вакансии")
            return None
        
        # Удаляем дубликаты
        unique_vacancies = self.remove_duplicates(all_vacancies)
        
        # Анализируем данные
        self.analyze_vacancies(unique_vacancies)
        
        # Сохраняем объединенный файл
        output_path = os.path.join(self.data_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(unique_vacancies, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Объединенный файл сохранен: {output_path}")
        
        # Создаем визуализации
        self.create_visualizations(unique_vacancies)
        
        # Генерируем отчет
        report_path = self.generate_report()
        
        print("=" * 60)
        print("✅ ОБЪЕДИНЕНИЕ ЗАВЕРШЕНО!")
        print("=" * 60)
        print(f"📁 Исходные файлы: {self.stats['total_files_processed']}")
        print(f"📊 Вакансий до: {self.stats['total_vacancies_before']:,}")
        print(f"📊 Вакансий после: {self.stats['total_vacancies_after']:,}")
        print(f"🔄 Дубликатов удалено: {self.stats['duplicates_removed']:,}")
        print(f"📄 Отчет: {report_path}")
        print(f"💾 Объединенный файл: {output_path}")
        
        return output_path


def main():
    """Основная функция для запуска объединения."""
    merger = VacancyMerger()
    
    try:
        # Запускаем объединение
        merged_file = merger.merge_and_analyze("FINAL_MERGED_INDUSTRIAL_VACANCIES.json")
        
        if merged_file:
            print(f"\n🎉 Процесс завершен успешно!")
            print(f"📈 Итоговая статистика:")
            print(f"   • Файлов обработано: {merger.stats['total_files_processed']}")
            print(f"   • Вакансий собрано: {merger.stats['total_vacancies_after']:,}")
            print(f"   • Дубликатов удалено: {merger.stats['duplicates_removed']:,}")
            print(f"   • Регионов охвачено: {merger.stats['regions_count']}")
            
            if merger.stats['date_range']['min']:
                date_range = merger.stats['date_range']
                print(f"   • Период данных: {date_range['min'].strftime('%Y-%m-%d')} - {date_range['max'].strftime('%Y-%m-%d')}")
        
    except Exception as e:
        print(f"❌ Ошибка при объединении файлов: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()