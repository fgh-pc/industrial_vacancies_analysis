"""
МОДУЛИ АНАЛИЗА ПРОМЫШЛЕННЫХ ВАКАНСИЙ
"""

from .industry_segments import analyze_industry_segments
from .position_levels import analyze_position_levels
from .salary_comparison import analyze_salary_comparison
from .dynamics import analyze_dynamics
from .professions_dynamics import analyze_professions_dynamics
from .regional import analyze_regional_distribution
from .skills import analyze_skills
from .forecast import analyze_forecast
from .dashboard import analyze_dashboard
from .report import save_text_report

__all__ = [
    'analyze_industry_segments',
    'analyze_position_levels',
    'analyze_salary_comparison',
    'analyze_dynamics',
    'analyze_professions_dynamics',
    'analyze_regional_distribution',
    'analyze_skills',
    'analyze_forecast',
    'analyze_dashboard',
    'save_text_report'
]

