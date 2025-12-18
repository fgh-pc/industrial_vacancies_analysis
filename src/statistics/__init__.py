"""
МОДУЛЬ СТАТИСТИЧЕСКИХ РАСЧЕТОВ
"""

from .error_estimation import (
    calculate_confidence_interval,
    calculate_proportion_confidence_interval,
    bootstrap_confidence_interval,
    calculate_statistical_summary,
    format_confidence_interval,
    format_proportion_confidence_interval
)

__all__ = [
    'calculate_confidence_interval',
    'calculate_proportion_confidence_interval',
    'bootstrap_confidence_interval',
    'calculate_statistical_summary',
    'format_confidence_interval',
    'format_proportion_confidence_interval'
]

