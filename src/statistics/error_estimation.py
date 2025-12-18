"""
МОДУЛЬ ОЦЕНКИ ПОГРЕШНОСТИ РЕЗУЛЬТАТОВ
Реализует методы статистической оценки погрешности: доверительные интервалы,
стандартную ошибку среднего, маржу ошибки и bootstrap-методы.
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, List, Optional
from scipy import stats


def calculate_confidence_interval(
    data: pd.Series,
    confidence_level: float = 0.95
) -> Dict[str, float]:
    """
    Вычисляет доверительный интервал для среднего значения.
    
    Использует t-распределение для малых выборок (n < 30) 
    и нормальное распределение для больших выборок.
    
    Args:
        data: Вектор данных
        confidence_level: Уровень доверия (по умолчанию 0.95 = 95%)
        
    Returns:
        Словарь с ключами:
        - mean: Среднее значение
        - std: Стандартное отклонение
        - n: Размер выборки
        - sem: Стандартная ошибка среднего
        - ci_lower: Нижняя граница доверительного интервала
        - ci_upper: Верхняя граница доверительного интервала
        - margin_of_error: Половина ширины интервала
        - confidence_level: Уровень доверия
    """
    # Убираем NaN и нулевые значения
    clean_data = data.dropna()
    clean_data = clean_data[clean_data > 0]
    
    if len(clean_data) == 0:
        return {
            'mean': 0.0,
            'std': 0.0,
            'n': 0,
            'sem': 0.0,
            'ci_lower': 0.0,
            'ci_upper': 0.0,
            'margin_of_error': 0.0,
            'confidence_level': confidence_level
        }
    
    n = len(clean_data)
    mean = float(clean_data.mean())
    std = float(clean_data.std(ddof=1))  # Смещенная оценка для выборки
    
    # Стандартная ошибка среднего
    sem = std / np.sqrt(n)
    
    # Выбираем распределение в зависимости от размера выборки
    if n < 30:
        # t-распределение для малых выборок
        alpha = 1 - confidence_level
        t_critical = stats.t.ppf(1 - alpha/2, df=n-1)
        margin_of_error = t_critical * sem
    else:
        # Нормальное распределение для больших выборок
        z_critical = stats.norm.ppf(1 - (1 - confidence_level) / 2)
        margin_of_error = z_critical * sem
    
    ci_lower = mean - margin_of_error
    ci_upper = mean + margin_of_error
    
    return {
        'mean': mean,
        'std': std,
        'n': n,
        'sem': sem,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'margin_of_error': margin_of_error,
        'confidence_level': confidence_level
    }


def calculate_proportion_confidence_interval(
    count: int,
    total: int,
    confidence_level: float = 0.95
) -> Dict[str, float]:
    """
    Вычисляет доверительный интервал для пропорции (процентного соотношения).
    
    Использует нормальное приближение биномиального распределения
    (применимо при n*p > 5 и n*(1-p) > 5).
    
    Args:
        count: Количество успешных исходов
        total: Общее количество наблюдений
        confidence_level: Уровень доверия (по умолчанию 0.95 = 95%)
        
    Returns:
        Словарь с ключами:
        - proportion: Доля (от 0 до 1)
        - percentage: Процент (от 0 до 100)
        - n: Размер выборки
        - ci_lower: Нижняя граница доверительного интервала (в процентах)
        - ci_upper: Верхняя граница доверительного интервала (в процентах)
        - margin_of_error: Маржа ошибки (в процентах)
        - confidence_level: Уровень доверия
    """
    if total == 0:
        return {
            'proportion': 0.0,
            'percentage': 0.0,
            'n': 0,
            'ci_lower': 0.0,
            'ci_upper': 0.0,
            'margin_of_error': 0.0,
            'confidence_level': confidence_level
        }
    
    proportion = count / total
    percentage = proportion * 100
    
    # Стандартная ошибка пропорции
    se_proportion = np.sqrt(proportion * (1 - proportion) / total)
    
    # Z-критическое значение для нормального распределения
    z_critical = stats.norm.ppf(1 - (1 - confidence_level) / 2)
    
    # Маржа ошибки
    margin_of_error = z_critical * se_proportion
    
    # Доверительный интервал для пропорции
    ci_lower_prop = max(0, proportion - margin_of_error)
    ci_upper_prop = min(1, proportion + margin_of_error)
    
    # Конвертируем в проценты
    ci_lower = ci_lower_prop * 100
    ci_upper = ci_upper_prop * 100
    margin_of_error_pct = margin_of_error * 100
    
    return {
        'proportion': proportion,
        'percentage': percentage,
        'n': total,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'margin_of_error': margin_of_error_pct,
        'confidence_level': confidence_level
    }


def bootstrap_confidence_interval(
    data: pd.Series,
    confidence_level: float = 0.95,
    n_bootstrap: int = 1000,
    statistic: str = 'mean'
) -> Dict[str, float]:
    """
    Вычисляет доверительный интервал с помощью bootstrap-метода.
    
    Bootstrap - это метод повторной выборки с возвращением,
    который позволяет оценить распределение статистики без предположений
    о форме распределения данных.
    
    Args:
        data: Вектор данных
        confidence_level: Уровень доверия (по умолчанию 0.95 = 95%)
        n_bootstrap: Количество bootstrap-выборок (по умолчанию 1000)
        statistic: Статистика для оценки ('mean', 'median', 'std')
        
    Returns:
        Словарь с ключами:
        - statistic_value: Значение статистики на исходных данных
        - ci_lower: Нижняя граница доверительного интервала
        - ci_upper: Верхняя граница доверительного интервала
        - n_bootstrap: Количество bootstrap-выборок
        - confidence_level: Уровень доверия
    """
    # Убираем NaN и нулевые значения
    clean_data = data.dropna()
    clean_data = clean_data[clean_data > 0]
    
    if len(clean_data) == 0:
        return {
            'statistic_value': 0.0,
            'ci_lower': 0.0,
            'ci_upper': 0.0,
            'n_bootstrap': n_bootstrap,
            'confidence_level': confidence_level
        }
    
    # Вычисляем статистику на исходных данных
    if statistic == 'mean':
        statistic_value = float(clean_data.mean())
        bootstrap_func = np.mean
    elif statistic == 'median':
        statistic_value = float(clean_data.median())
        bootstrap_func = np.median
    elif statistic == 'std':
        statistic_value = float(clean_data.std(ddof=1))
        bootstrap_func = lambda x: np.std(x, ddof=1)
    else:
        raise ValueError(f"Неизвестная статистика: {statistic}")
    
    # Bootstrap-выборки
    bootstrap_statistics = []
    n = len(clean_data)
    
    for _ in range(n_bootstrap):
        # Случайная выборка с возвращением
        bootstrap_sample = np.random.choice(clean_data, size=n, replace=True)
        bootstrap_stat = bootstrap_func(bootstrap_sample)
        bootstrap_statistics.append(bootstrap_stat)
    
    bootstrap_statistics = np.array(bootstrap_statistics)
    
    # Вычисляем процентили для доверительного интервала
    alpha = 1 - confidence_level
    lower_percentile = (alpha / 2) * 100
    upper_percentile = (1 - alpha / 2) * 100
    
    ci_lower = float(np.percentile(bootstrap_statistics, lower_percentile))
    ci_upper = float(np.percentile(bootstrap_statistics, upper_percentile))
    
    return {
        'statistic_value': statistic_value,
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
        'n_bootstrap': n_bootstrap,
        'confidence_level': confidence_level
    }


def calculate_statistical_summary(
    data: pd.Series,
    confidence_level: float = 0.95
) -> Dict[str, any]:
    """
    Вычисляет полную статистическую сводку с оценкой погрешности.
    
    Включает:
    - Базовые статистики (среднее, медиана, стандартное отклонение)
    - Доверительные интервалы (t-распределение и bootstrap)
    - Стандартную ошибку среднего
    
    Args:
        data: Вектор данных
        confidence_level: Уровень доверия (по умолчанию 0.95 = 95%)
        
    Returns:
        Словарь с полной статистической информацией
    """
    # Убираем NaN и нулевые значения
    clean_data = data.dropna()
    clean_data = clean_data[clean_data > 0]
    
    if len(clean_data) == 0:
        return {
            'n': 0,
            'mean': 0.0,
            'median': 0.0,
            'std': 0.0,
            'min': 0.0,
            'max': 0.0,
            'confidence_interval': {},
            'bootstrap_confidence_interval': {},
            'sem': 0.0
        }
    
    # Базовые статистики
    n = len(clean_data)
    mean = float(clean_data.mean())
    median = float(clean_data.median())
    std = float(clean_data.std(ddof=1))
    min_val = float(clean_data.min())
    max_val = float(clean_data.max())
    
    # Доверительный интервал (t-распределение)
    ci = calculate_confidence_interval(clean_data, confidence_level)
    
    # Bootstrap доверительный интервал
    bootstrap_ci = bootstrap_confidence_interval(
        clean_data, 
        confidence_level=confidence_level,
        n_bootstrap=1000,
        statistic='mean'
    )
    
    return {
        'n': n,
        'mean': mean,
        'median': median,
        'std': std,
        'min': min_val,
        'max': max_val,
        'confidence_interval': ci,
        'bootstrap_confidence_interval': bootstrap_ci,
        'sem': ci['sem']
    }


def format_confidence_interval(
    ci: Dict[str, float],
    unit: str = 'руб',
    precision: int = 0
) -> str:
    """
    Форматирует доверительный интервал для вывода.
    
    Args:
        ci: Словарь с результатами calculate_confidence_interval
        unit: Единица измерения (по умолчанию 'руб')
        precision: Количество знаков после запятой
        
    Returns:
        Отформатированная строка вида "mean [ci_lower, ci_upper] unit"
    """
    mean = ci['mean']
    lower = ci['ci_lower']
    upper = ci['ci_upper']
    level = int(ci['confidence_level'] * 100)
    
    if precision == 0:
        return f"{mean:,.0f} [{lower:,.0f}, {upper:,.0f}] {unit} (95% ДИ)"
    else:
        return f"{mean:,.{precision}f} [{lower:,.{precision}f}, {upper:,.{precision}f}] {unit} (95% ДИ)"


def format_proportion_confidence_interval(
    ci: Dict[str, float],
    precision: int = 2
) -> str:
    """
    Форматирует доверительный интервал для пропорции.
    
    Args:
        ci: Словарь с результатами calculate_proportion_confidence_interval
        precision: Количество знаков после запятой
        
    Returns:
        Отформатированная строка вида "percentage% [ci_lower%, ci_upper%]"
    """
    percentage = ci['percentage']
    lower = ci['ci_lower']
    upper = ci['ci_upper']
    level = int(ci['confidence_level'] * 100)
    
    return f"{percentage:.{precision}f}% [{lower:.{precision}f}%, {upper:.{precision}f}%] ({level}% ДИ)"

