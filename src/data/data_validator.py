"""
Модуль для валидации и проверки качества данных.
Содержит функции для проверки целостности, согласованности и качества данных.
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime
import re
import os

class DataValidator:
    """
    Класс для валидации данных о вакансиях.
    Проверяет качество, целостность и согласованность данных.
    """
    
    def __init__(self):
        self.logger = self._setup_logger()
        self.validation_results = {}
        
    def _setup_logger(self):
        """Настройка логирования."""
        logger = logging.getLogger('DataValidator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
        
    def validate_dataset(self, df: pd.DataFrame) -> Dict:
        """
        Полная валидация датасета.
        
        Args:
            df: DataFrame для валидации
            
        Returns:
            Dict: Результаты валидации
        """
        self.logger.info("Начинаем полную валидацию датасета...")
        
        validation_report = {
            "summary": {},
            "quality_metrics": {},
            "issues": {},
            "recommendations": []
        }
        
        # Базовые проверки
        validation_report["basic_checks"] = self._perform_basic_checks(df)
        
        # Проверки качества данных
        validation_report["quality_checks"] = self._perform_quality_checks(df)
        
        # Проверки целостности
        validation_report["integrity_checks"] = self._perform_integrity_checks(df)
        
        # Проверки согласованности
        validation_report["consistency_checks"] = self._perform_consistency_checks(df)
        
        # Генерация сводки - ИСПРАВЛЕНО: передаем df
        validation_report["summary"] = self._generate_validation_summary(validation_report, df)
        
        # Рекомендации по улучшению
        validation_report["recommendations"] = self._generate_recommendations(validation_report)
        
        self.validation_results = validation_report
        return validation_report
        
    def _perform_basic_checks(self, df: pd.DataFrame) -> Dict:
        """Выполнение базовых проверок датасета."""
        checks = {}
        
        # Проверка размера датасета
        checks["dataset_size"] = {
            "rows": len(df),
            "columns": len(df.columns),
            "status": "PASS" if len(df) > 0 else "FAIL"
        }
        
        # Проверка наличия обязательных столбцов
        required_columns = ['id', 'name', 'published_at']
        missing_columns = [col for col in required_columns if col not in df.columns]
        checks["required_columns"] = {
            "missing": missing_columns,
            "status": "PASS" if not missing_columns else "FAIL"
        }
        
        # Проверка типов данных
        type_issues = []
        if 'id' in df.columns and not pd.api.types.is_numeric_dtype(df['id']):
            type_issues.append("id должен быть числовым")
        if 'published_at' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['published_at']):
            type_issues.append("published_at должен быть datetime")
            
        checks["data_types"] = {
            "issues": type_issues,
            "status": "PASS" if not type_issues else "WARNING"
        }
        
        return checks
        
    def _perform_quality_checks(self, df: pd.DataFrame) -> Dict:
        """Проверка качества данных."""
        quality_metrics = {}
        
        # Пропущенные значения
        missing_data = {}
        for column in df.columns:
            missing_count = df[column].isna().sum()
            missing_percentage = (missing_count / len(df)) * 100
            missing_data[column] = {
                "count": int(missing_count),
                "percentage": round(missing_percentage, 2)
            }
            
        quality_metrics["missing_values"] = missing_data
        
        # Дубликаты
        duplicate_count = df.duplicated(subset=['id']).sum()
        quality_metrics["duplicates"] = {
            "count": int(duplicate_count),
            "percentage": round((duplicate_count / len(df)) * 100, 2)
        }
        
        # Выбросы в зарплатах
        if 'salary_avg_rub' in df.columns:
            salary_data = df['salary_avg_rub'].dropna()
            if len(salary_data) > 0:
                Q1 = salary_data.quantile(0.25)
                Q3 = salary_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = salary_data[(salary_data < lower_bound) | (salary_data > upper_bound)]
                quality_metrics["salary_outliers"] = {
                    "count": len(outliers),
                    "percentage": round((len(outliers) / len(salary_data)) * 100, 2),
                    "bounds": {"lower": lower_bound, "upper": upper_bound}
                }
        
        # Качество текстовых полей
        text_quality = {}
        text_columns = ['name', 'area.name', 'employer.name']
        
        for column in text_columns:
            if column in df.columns:
                # Проверка пустых строк
                empty_strings = (df[column].astype(str).str.strip() == '').sum()
                # Проверка стандартных значений "не указано"
                default_values = (df[column].astype(str).str.lower() == 'не указано').sum()
                
                text_quality[column] = {
                    "empty_strings": int(empty_strings),
                    "default_values": int(default_values),
                    "unique_values": df[column].nunique()
                }
                
        quality_metrics["text_quality"] = text_quality
        
        return quality_metrics
        
    def _perform_integrity_checks(self, df: pd.DataFrame) -> Dict:
        """Проверка целостности данных."""
        integrity_checks = {}
        
        # Проверка временных меток - ИСПРАВЛЕННАЯ ВЕРСИЯ
        if 'published_at' in df.columns:
            try:
                # Убедимся что published_at это datetime
                if not pd.api.types.is_datetime64_any_dtype(df['published_at']):
                    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
                
                # Приводим к одному часовому поясу для сравнения
                if df['published_at'].dt.tz is not None:
                    current_time = pd.Timestamp.now(tz=df['published_at'].dt.tz)
                else:
                    current_time = pd.Timestamp.now(tz='UTC')
                    df['published_at'] = df['published_at'].dt.tz_localize('UTC')
                
                future_dates = (df['published_at'] > current_time).sum()
                
                integrity_checks["future_dates"] = {
                    "count": int(future_dates),
                    "status": "PASS" if future_dates == 0 else "FAIL"
                }
            except Exception as e:
                self.logger.warning(f"Проверка дат пропущена: {e}")
                integrity_checks["future_dates"] = {
                    "count": 0,
                    "status": "SKIPPED"
                }
            
        # Проверка логической целостности зарплат
        if all(col in df.columns for col in ['salary_from_rub', 'salary_to_rub']):
            # Преобразуем к numeric на случай смешанных типов
            salary_from = pd.to_numeric(df['salary_from_rub'], errors='coerce')
            salary_to = pd.to_numeric(df['salary_to_rub'], errors='coerce')
            invalid_salary_ranges = (salary_from > salary_to).sum()
            integrity_checks["salary_ranges"] = {
                "invalid_ranges": int(invalid_salary_ranges),
                "status": "PASS" if invalid_salary_ranges == 0 else "FAIL"
            }
            
        # Проверка уникальности ID
        if 'id' in df.columns:
            unique_ids = df['id'].nunique()
            integrity_checks["unique_ids"] = {
                "unique_count": int(unique_ids),
                "total_count": len(df),
                "status": "PASS" if unique_ids == len(df) else "FAIL"
            }
            
        return integrity_checks
        
    def _perform_consistency_checks(self, df: pd.DataFrame) -> Dict:
        """Проверка согласованности данных."""
        consistency_checks = {}
        
        # Согласованность категориальных переменных
        if 'position_level' in df.columns:
            valid_levels = ['worker', 'specialist', 'engineer', 'leadership', 'executive', 'other', 'unknown']
            invalid_levels = set(df['position_level']) - set(valid_levels)
            consistency_checks["position_levels"] = {
                "invalid_values": list(invalid_levels),
                "status": "PASS" if not invalid_levels else "FAIL"
            }
            
        # Согласованность отраслевых сегментов
        if 'industry_segment' in df.columns:
            valid_segments = ['machinery', 'metallurgy', 'chemical', 'energy', 'oil_gas', 'construction', 'other_industry']
            invalid_segments = set(df['industry_segment']) - set(valid_segments)
            consistency_checks["industry_segments"] = {
                "invalid_values": list(invalid_segments),
                "status": "PASS" if not invalid_segments else "FAIL"
            }
            
        # Проверка согласованности дат - ИСПРАВЛЕННАЯ ЧАСТЬ
        if 'published_at' in df.columns and 'published_year' in df.columns:
            try:
                # Убедимся что published_at это datetime
                if not pd.api.types.is_datetime64_any_dtype(df['published_at']):
                    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
                
                year_mismatch = (df['published_at'].dt.year != df['published_year']).sum()
                consistency_checks["date_consistency"] = {
                    "mismatches": int(year_mismatch),
                    "status": "PASS" if year_mismatch == 0 else "FAIL"
                }
            except Exception as e:
                self.logger.warning(f"Проверка согласованности дат пропущена: {e}")
                consistency_checks["date_consistency"] = {
                    "mismatches": 0,
                    "status": "SKIPPED"
                }
            
        return consistency_checks
        
    def _generate_validation_summary(self, validation_report: Dict, df: pd.DataFrame = None) -> Dict:
        """Генерация сводки по результатам валидации."""
        summary = {
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "warning_checks": 0,
            "overall_score": 0
        }
        
        # Анализируем результаты проверок
        check_categories = ['basic_checks', 'integrity_checks', 'consistency_checks']
        
        for category in check_categories:
            if category in validation_report:
                for check_name, check_result in validation_report[category].items():
                    summary["total_checks"] += 1
                    status = check_result.get("status", "UNKNOWN")
                    
                    if status == "PASS":
                        summary["passed_checks"] += 1
                    elif status == "FAIL":
                        summary["failed_checks"] += 1
                    elif status == "WARNING":
                        summary["warning_checks"] += 1
                    # SKIPPED проверки не учитываются в общем счете
        
        # Расчет общего скора
        if summary["total_checks"] > 0:
            summary["overall_score"] = round(
                (summary["passed_checks"] / summary["total_checks"]) * 100, 2
            )
            
        # Качество данных - ИСПРАВЛЕНО: проверяем наличие df
        if df is not None:
            quality_metrics = validation_report.get("quality_checks", {})
            if "missing_values" in quality_metrics:
                total_missing = sum(
                    info["count"] for info in quality_metrics["missing_values"].values()
                )
                total_cells = len(df) * len(df.columns)
                missing_percentage = (total_missing / total_cells) * 100 if total_cells > 0 else 0
                summary["data_completeness"] = round(100 - missing_percentage, 2)
        else:
            summary["data_completeness"] = 0
            
        return summary
        
    def _generate_recommendations(self, validation_report: Dict) -> List[str]:
        """Генерация рекомендаций по улучшению данных."""
        recommendations = []
        
        # Анализ проблем с пропущенными значениями
        quality_checks = validation_report.get("quality_checks", {})
        if "missing_values" in quality_checks:
            high_missing_columns = []
            for column, info in quality_checks["missing_values"].items():
                if info["percentage"] > 50:  # Более 50% пропусков
                    high_missing_columns.append(f"{column} ({info['percentage']}%)")
                    
            if high_missing_columns:
                recommendations.append(
                    f"Рассмотреть удаление столбцов с высоким процентом пропусков: {', '.join(high_missing_columns)}"
                )
                
        # Рекомендации по дубликатам
        if "duplicates" in quality_checks:
            dup_percentage = quality_checks["duplicates"]["percentage"]
            if dup_percentage > 5:
                recommendations.append(
                    f"Обнаружено {dup_percentage}% дубликатов. Рекомендуется проверить процесс сбора данных."
                )
                
        # Рекомендации по выбросам
        if "salary_outliers" in quality_checks:
            outlier_percentage = quality_checks["salary_outliers"]["percentage"]
            if outlier_percentage > 5:
                recommendations.append(
                    f"Обнаружено {outlier_percentage}% выбросов в зарплатах. Рекомендуется анализ аномалий."
                )
                
        # Рекомендации по целостности
        integrity_checks = validation_report.get("integrity_checks", {})
        if "future_dates" in integrity_checks and integrity_checks["future_dates"]["status"] == "FAIL":
            recommendations.append(
                f"Обнаружены вакансии с датами из будущего: {integrity_checks['future_dates']['count']} шт."
            )
            
        if "salary_ranges" in integrity_checks and integrity_checks["salary_ranges"]["status"] == "FAIL":
            recommendations.append(
                f"Обнаружены нелогичные диапазоны зарплат: {integrity_checks['salary_ranges']['count']} шт."
            )
            
        # Общие рекомендации
        summary = validation_report.get("summary", {})
        if summary.get("overall_score", 0) < 80:
            recommendations.append(
                f"Общий score качества данных {summary['overall_score']}%. Требуется улучшение качества данных."
            )
            
        if not recommendations:
            recommendations.append("Качество данных удовлетворительное. Рекомендаций по улучшению нет.")
            
        return recommendations
        
    def save_validation_report(self, report: Dict, output_path: str):
        """
        Сохранение отчета о валидации.
        
        Args:
            report: Отчет о валидации
            output_path: Путь для сохранения
        """
        try:
            # Создаем папку если её нет
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Отчет о валидации сохранен: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения отчета: {e}")
            
    def generate_data_quality_dashboard(self, df: pd.DataFrame, output_path: str):
        """
        Генерация визуальной панели качества данных.
        
        Args:
            df: DataFrame для анализа
            output_path: Путь для сохранения dashboard
        """
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            # Создаем папку если её нет
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('Дэшборд Качества Данных Вакансий', fontsize=16, fontweight='bold')
            
            # 1. Пропущенные значения по столбцам
            missing_data = df.isnull().sum()
            missing_percentage = (missing_data / len(df)) * 100
            
            # Топ-10 столбцов с пропусками
            top_missing = missing_percentage.nlargest(10)
            axes[0, 0].barh(range(len(top_missing)), top_missing.values)
            axes[0, 0].set_yticks(range(len(top_missing)))
            axes[0, 0].set_yticklabels(top_missing.index)
            axes[0, 0].set_title('Топ-10 столбцов с пропущенными значениями')
            axes[0, 0].set_xlabel('Процент пропусков (%)')
            
            # 2. Распределение типов данных
            dtype_counts = df.dtypes.value_counts()
            axes[0, 1].pie(dtype_counts.values, labels=dtype_counts.index, autopct='%1.1f%%')
            axes[0, 1].set_title('Распределение типов данных')
            
            # 3. Распределение зарплат (если есть)
            if 'salary_avg_rub' in df.columns:
                salary_data = df['salary_avg_rub'].dropna()
                if len(salary_data) > 0:
                    axes[1, 0].hist(salary_data, bins=50, alpha=0.7, edgecolor='black')
                    axes[1, 0].axvline(salary_data.mean(), color='red', linestyle='--', label=f'Среднее: {salary_data.mean():.0f}')
                    axes[1, 0].axvline(salary_data.median(), color='green', linestyle='--', label=f'Медиана: {salary_data.median():.0f}')
                    axes[1, 0].set_title('Распределение зарплат')
                    axes[1, 0].set_xlabel('Зарплата (руб)')
                    axes[1, 0].set_ylabel('Количество')
                    axes[1, 0].legend()
                    
            # 4. Временное распределение (если есть дата)
            if 'published_at' in df.columns:
                # Убедимся что это datetime
                if pd.api.types.is_datetime64_any_dtype(df['published_at']):
                    try:
                        monthly_counts = df['published_at'].dt.to_period('M').value_counts().sort_index()
                        axes[1, 1].plot(monthly_counts.index.astype(str), monthly_counts.values, marker='o')
                        axes[1, 1].set_title('Динамика публикации вакансий')
                        axes[1, 1].set_xlabel('Месяц')
                        axes[1, 1].set_ylabel('Количество вакансий')
                        axes[1, 1].tick_params(axis='x', rotation=45)
                    except Exception as e:
                        self.logger.warning(f"Не удалось построить временной график: {e}")
                        axes[1, 1].text(0.5, 0.5, 'Не удалось построить график\nвременного распределения', 
                                       ha='center', va='center', transform=axes[1, 1].transAxes)
                        axes[1, 1].set_title('Динамика публикации вакансий')
                
            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Дэшборд качества данных сохранен: {output_path}")
            
        except ImportError:
            self.logger.warning("Matplotlib/Seaborn не установлены. Невозможно создать дэшборд.")
        except Exception as e:
            self.logger.error(f"Ошибка создания дэшборда: {e}")


# Функция для полной валидации датасета
def validate_complete_dataset(df: pd.DataFrame, output_base_name: str = None):
    """
    Полная валидация датасета с сохранением отчета.
    
    Args:
        df: DataFrame для валидации
        output_base_name: Базовое имя для выходных файлов
    """
    if not output_base_name:
        output_base_name = f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    validator = DataValidator()
    
    # СОЗДАЕМ ПАПКИ ПЕРЕД СОХРАНЕНИЕМ - ДОБАВЛЕННАЯ ЧАСТЬ
    os.makedirs("reports/validation", exist_ok=True)
    
    # Выполняем валидацию
    validation_report = validator.validate_dataset(df)
    
    # Сохраняем отчет
    report_path = f"reports/validation/{output_base_name}.json"
    validator.save_validation_report(validation_report, report_path)
    
    # Создаем дэшборд
    dashboard_path = f"reports/validation/{output_base_name}_dashboard.png"
    validator.generate_data_quality_dashboard(df, dashboard_path)
    
    # Выводим сводку
    summary = validation_report.get("summary", {})
    print("\n" + "="*50)
    print("СВОДКА ВАЛИДАЦИИ ДАННЫХ")
    print("="*50)
    print(f"Общий score: {summary.get('overall_score', 0)}%")
    print(f"Пройдено проверок: {summary.get('passed_checks', 0)}/{summary.get('total_checks', 0)}")
    print(f"Полнота данных: {summary.get('data_completeness', 0)}%")
    
    recommendations = validation_report.get("recommendations", [])
    if recommendations:
        print("\nРЕКОМЕНДАЦИИ:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
            
    print(f"\nПодробный отчет сохранен: {report_path}")
    
    return validation_report


if __name__ == "__main__":
    # Пример использования
    import pandas as pd
    
    # Загрузка данных для тестирования - ИСПРАВЛЕННАЯ ЧАСТЬ
    try:
        # Загружаем с правильными настройками для mixed types
        df = pd.read_csv(
            "data/processed/cleaned_dataset_20251031_005056.csv",
            low_memory=False,  # Решает проблему mixed types
            dtype={'id': 'str'}  # Явно указываем тип для ID
        )
        
        # Принудительно преобразуем даты
        df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce', utc=True)
        
        # Принудительно преобразуем числовые столбцы
        numeric_columns = ['salary_from_rub', 'salary_to_rub', 'salary_avg_rub', 'skills_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        validate_complete_dataset(df)
        
    except Exception as e:
        print(f"Ошибка: {e}")