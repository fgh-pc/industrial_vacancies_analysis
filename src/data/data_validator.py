"""
Модуль для валидации и проверки качества данных.
Улучшенная версия с исправлениями и оптимизацией.
"""

import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime, timezone
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
    
    def _safe_datetime_conversion(self, df: pd.DataFrame, column: str) -> pd.Series:
        """Безопасное преобразование в datetime."""
        if column not in df.columns:
            return pd.Series([], dtype='datetime64[ns]')
        
        try:
            # Пробуем разные форматы дат
            return pd.to_datetime(
                df[column], 
                errors='coerce', 
                utc=True,
                infer_datetime_format=True
            )
        except Exception as e:
            self.logger.warning(f"Ошибка преобразования {column}: {e}")
            return pd.Series([np.nan] * len(df), dtype='datetime64[ns]')
    
    def _safe_numeric_conversion(self, series: pd.Series) -> pd.Series:
        """Безопасное преобразование в числовой формат."""
        try:
            return pd.to_numeric(series, errors='coerce')
        except Exception as e:
            self.logger.warning(f"Ошибка преобразования в numeric: {e}")
            return pd.Series([np.nan] * len(series))
        
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
        
        try:
            # Базовые проверки
            validation_report["basic_checks"] = self._perform_basic_checks(df)
            
            # Проверки качества данных
            validation_report["quality_checks"] = self._perform_quality_checks(df)
            
            # Проверки целостности
            validation_report["integrity_checks"] = self._perform_integrity_checks(df)
            
            # Проверки согласованности
            validation_report["consistency_checks"] = self._perform_consistency_checks(df)
            
            # Генерация сводки
            validation_report["summary"] = self._generate_validation_summary(validation_report, df)
            
            # Рекомендации по улучшению
            validation_report["recommendations"] = self._generate_recommendations(validation_report)
            
            self.validation_results = validation_report
            return validation_report
            
        except Exception as e:
            self.logger.error(f"Ошибка при валидации: {e}")
            validation_report["error"] = str(e)
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
        
        # Проверка типов данных (упрощенная)
        type_issues = []
        if 'id' in df.columns:
            # Проверяем что ID уникальны и не пустые
            unique_ids = df['id'].nunique()
            if unique_ids != len(df):
                type_issues.append(f"ID не уникальны: {unique_ids}/{len(df)}")
                
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
        important_columns = ['id', 'name', 'area', 'published_at', 'employer_name']
        
        for column in important_columns:
            if column in df.columns:
                missing_count = df[column].isna().sum()
                missing_percentage = (missing_count / len(df)) * 100
                missing_data[column] = {
                    "count": int(missing_count),
                    "percentage": round(missing_percentage, 2)
                }
                
        quality_metrics["missing_values"] = missing_data
        
        # Дубликаты
        duplicate_count = 0
        if 'id' in df.columns:
            duplicate_count = df.duplicated(subset=['id']).sum()
            
        quality_metrics["duplicates"] = {
            "count": int(duplicate_count),
            "percentage": round((duplicate_count / len(df)) * 100, 2) if len(df) > 0 else 0
        }
        
        # Выбросы в зарплатах (упрощенная версия)
        if 'salary_avg_rub' in df.columns:
            salary_data = self._safe_numeric_conversion(df['salary_avg_rub']).dropna()
            if len(salary_data) > 10:  # Минимум 10 значений для анализа
                # Простой метод - отсекаем крайние 1%
                lower_bound = salary_data.quantile(0.01)
                upper_bound = salary_data.quantile(0.99)
                
                outliers = salary_data[(salary_data < lower_bound) | (salary_data > upper_bound)]
                quality_metrics["salary_outliers"] = {
                    "count": len(outliers),
                    "percentage": round((len(outliers) / len(salary_data)) * 100, 2),
                    "bounds": {"lower": float(lower_bound), "upper": float(upper_bound)}
                }
        
        return quality_metrics
        
    def _perform_integrity_checks(self, df: pd.DataFrame) -> Dict:
        """Проверка целостности данных."""
        integrity_checks = {}
        
        # Проверка временных меток (упрощенная)
        if 'published_at' in df.columns:
            published_dates = self._safe_datetime_conversion(df, 'published_at')
            current_time = pd.Timestamp.now(tz='UTC')
            
            # Будущие даты (с запасом +1 день на случай разных часовых поясов)
            future_dates = (published_dates > current_time + pd.Timedelta(days=1)).sum()
            
            integrity_checks["future_dates"] = {
                "count": int(future_dates),
                "status": "PASS" if future_dates == 0 else "WARNING"
            }
            
        # Проверка логической целостности зарплат
        if all(col in df.columns for col in ['salary_from_rub', 'salary_to_rub']):
            salary_from = self._safe_numeric_conversion(df['salary_from_rub'])
            salary_to = self._safe_numeric_conversion(df['salary_to_rub'])
            
            # Только где оба значения не NaN
            valid_mask = salary_from.notna() & salary_to.notna()
            invalid_ranges = (salary_from[valid_mask] > salary_to[valid_mask]).sum()
            
            integrity_checks["salary_ranges"] = {
                "invalid_ranges": int(invalid_ranges),
                "status": "PASS" if invalid_ranges == 0 else "WARNING"
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
        categorical_checks = {}
        
        if 'position_level' in df.columns:
            valid_levels = ['worker', 'specialist', 'engineer', 'leadership', 'executive', 'other', 'unknown']
            invalid_levels = [x for x in df['position_level'].unique() if x not in valid_levels]
            categorical_checks["position_levels"] = {
                "invalid_values": invalid_levels,
                "status": "PASS" if not invalid_levels else "WARNING"
            }
            
        if 'industry_segment' in df.columns:
            valid_segments = ['machinery', 'metallurgy', 'chemical', 'energy', 'oil_gas', 'construction', 'other_industry']
            invalid_segments = [x for x in df['industry_segment'].unique() if x not in valid_segments]
            categorical_checks["industry_segments"] = {
                "invalid_values": invalid_segments,
                "status": "PASS" if not invalid_segments else "WARNING"
            }
            
        consistency_checks["categorical_consistency"] = categorical_checks
        
        return consistency_checks
        
    def _generate_validation_summary(self, validation_report: Dict, df: pd.DataFrame = None) -> Dict:
        """Генерация сводки по результатам валидации."""
        summary = {
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 0,
            "warning_checks": 0,
            "overall_score": 0,
            "data_completeness": 0
        }
        
        # Анализируем результаты проверок
        check_categories = ['basic_checks', 'integrity_checks', 'consistency_checks']
        
        for category in check_categories:
            if category in validation_report:
                for check_name, check_result in validation_report[category].items():
                    if isinstance(check_result, dict) and 'status' in check_result:
                        summary["total_checks"] += 1
                        status = check_result["status"]
                        
                        if status == "PASS":
                            summary["passed_checks"] += 1
                        elif status == "FAIL":
                            summary["failed_checks"] += 1
                        elif status == "WARNING":
                            summary["warning_checks"] += 1
        
        # Расчет общего скора
        if summary["total_checks"] > 0:
            summary["overall_score"] = round(
                (summary["passed_checks"] / summary["total_checks"]) * 100, 2
            )
            
        # Качество данных
        if df is not None:
            important_columns = ['id', 'name', 'area', 'published_at']
            available_columns = [col for col in important_columns if col in df.columns]
            
            if available_columns:
                total_cells = len(df) * len(available_columns)
                if total_cells > 0:
                    missing_cells = sum(df[col].isna().sum() for col in available_columns)
                    completeness = 100 - (missing_cells / total_cells * 100)
                    summary["data_completeness"] = round(completeness, 2)
            
        return summary
        
    def _generate_recommendations(self, validation_report: Dict) -> List[str]:
        """Генерация рекомендаций по улучшению данных."""
        recommendations = []
        
        # Анализ проблем с пропущенными значениями
        quality_checks = validation_report.get("quality_checks", {})
        if "missing_values" in quality_checks:
            high_missing_columns = []
            for column, info in quality_checks["missing_values"].items():
                if info["percentage"] > 20:  # Более 20% пропусков
                    high_missing_columns.append(f"{column} ({info['percentage']}%)")
                    
            if high_missing_columns:
                recommendations.append(
                    f"Столбцы с высоким процентом пропусков: {', '.join(high_missing_columns)}"
                )
                
        # Рекомендации по дубликатам
        if "duplicates" in quality_checks:
            dup_percentage = quality_checks["duplicates"]["percentage"]
            if dup_percentage > 1:
                recommendations.append(
                    f"Обнаружено {dup_percentage}% дубликатов. Рекомендуется очистка данных."
                )
                
        # Общие рекомендации
        summary = validation_report.get("summary", {})
        overall_score = summary.get("overall_score", 0)
        
        if overall_score < 70:
            recommendations.append(f"Низкий score качества данных ({overall_score}%). Требуется улучшение.")
        elif overall_score < 90:
            recommendations.append(f"Удовлетворительный score качества ({overall_score}%). Возможны улучшения.")
        else:
            recommendations.append("Отличное качество данных!")
            
        if not recommendations:
            recommendations.append("Качество данных удовлетворительное.")
            
        return recommendations
        
    def save_validation_report(self, report: Dict, output_path: str):
        """
        Сохранение отчета о валидации.
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
                
            self.logger.info(f"Отчет о валидации сохранен: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения отчета: {e}")
            
    def generate_data_quality_dashboard(self, df: pd.DataFrame, output_path: str):
        """
        Генерация визуальной панели качества данных.
        """
        try:
            import matplotlib.pyplot as plt
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('Дэшборд Качества Данных Вакансий', fontsize=16, fontweight='bold')
            
            # 1. Пропущенные значения
            missing_data = df.isnull().sum()
            missing_percentage = (missing_data / len(df)) * 100
            
            # Топ-10 столбцов с пропусками
            top_missing = missing_percentage.nlargest(10)
            if len(top_missing) > 0:
                axes[0, 0].barh(range(len(top_missing)), top_missing.values)
                axes[0, 0].set_yticks(range(len(top_missing)))
                axes[0, 0].set_yticklabels([str(x)[:20] for x in top_missing.index])
            axes[0, 0].set_title('Топ-10 столбцов с пропущенными значениями')
            axes[0, 0].set_xlabel('Процент пропусков (%)')
            
            # 2. Распределение типов данных
            dtype_counts = df.dtypes.value_counts()
            if len(dtype_counts) > 0:
                axes[0, 1].pie(dtype_counts.values, labels=dtype_counts.index, autopct='%1.1f%%')
            axes[0, 1].set_title('Распределение типов данных')
            
            # 3. Распределение зарплат
            if 'salary_avg_rub' in df.columns:
                salary_data = self._safe_numeric_conversion(df['salary_avg_rub']).dropna()
                if len(salary_data) > 0:
                    axes[1, 0].hist(salary_data, bins=30, alpha=0.7, edgecolor='black')
                    axes[1, 0].set_title('Распределение зарплат')
                    axes[1, 0].set_xlabel('Зарплата (руб)')
                    axes[1, 0].set_ylabel('Количество')
            
            # 4. Простая статистика
            stats_text = f"Всего вакансий: {len(df)}\n"
            stats_text += f"Колонок: {len(df.columns)}\n"
            
            if 'salary_avg_rub' in df.columns:
                salary_data = self._safe_numeric_conversion(df['salary_avg_rub']).dropna()
                if len(salary_data) > 0:
                    stats_text += f"Ср. зарплата: {salary_data.mean():.0f} руб\n"
                    stats_text += f"Мед. зарплата: {salary_data.median():.0f} руб"
            
            axes[1, 1].text(0.5, 0.5, stats_text, ha='center', va='center', 
                          transform=axes[1, 1].transAxes, fontsize=12)
            axes[1, 1].set_title('Основная статистика')
            axes[1, 1].axis('off')
                
            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Дэшборд качества данных сохранен: {output_path}")
            
        except ImportError:
            self.logger.warning("Matplotlib не установлен. Невозможно создать дэшборд.")
        except Exception as e:
            self.logger.error(f"Ошибка создания дэшборда: {e}")


def validate_complete_dataset(df: pd.DataFrame, output_base_name: str = None):
    """
    Полная валидация датасета с сохранением отчета.
    """
    if not output_base_name:
        output_base_name = f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    validator = DataValidator()
    
    # Создаем папки
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


# Тестирование
if __name__ == "__main__":
    # Создаем тестовые данные
    test_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Инженер', 'Разработчик', 'Аналитик', 'Тестировщик', 'Менеджер'],
        'area': ['Москва', 'СПб', 'Москва', None, 'Москва'],
        'salary_avg_rub': [100000, 150000, 120000, 80000, 200000],
        'published_at': [
            '2024-01-01', '2024-01-02', '2024-01-03', 
            '2024-01-04', '2024-01-05'
        ],
        'position_level': ['engineer', 'engineer', 'specialist', 'specialist', 'leadership']
    }
    
    df_test = pd.DataFrame(test_data)
    df_test['published_at'] = pd.to_datetime(df_test['published_at'])
    
    print(" Тестирование DataValidator...")
    report = validate_complete_dataset(df_test, "test_validation")
    print("[V] Тестирование завершено!")