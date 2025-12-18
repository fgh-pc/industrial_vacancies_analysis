import json
from pathlib import Path

from vacancy_merger import VacancyMerger


def test_vacancy_merger_deduplicates_and_reports(tmp_path: Path):
    """Проверяет объединение JSON, удаление дублей и генерацию отчета без реальных данных."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    file1 = data_dir / "part1.json"
    file2 = data_dir / "part2.json"

    file1.write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "name": "Инженер",
                    "area": {"name": "Москва"},
                    "salary": {"from": 50000, "to": 70000},
                    "published_at": "2024-01-01T00:00:00+0300",
                    "collection_method": "api",
                },
                {
                    "id": 2,
                    "name": "Слесарь",
                    "area": {"name": "СПб"},
                    "salary": {"from": 60000, "to": 80000},
                    "published_at": "2024-01-02T00:00:00+0300",
                    "collection_method": "api",
                },
            ]
        ),
        encoding="utf-8",
    )

    file2.write_text(
        json.dumps(
            [
                {
                    "id": 2,  # дубликат
                    "name": "Слесарь",
                    "area": {"name": "СПб"},
                    "salary": {"from": 60000, "to": 80000},
                    "published_at": "2024-01-02T00:00:00+0300",
                    "collection_method": "api",
                },
                {
                    "id": 3,
                    "name": "Энергетик",
                    "area": {"name": "Москва"},
                    "salary": {"from": 90000, "to": 110000},
                    "published_at": "2024-01-03T00:00:00+0300",
                    "collection_method": "scraper",
                },
            ]
        ),
        encoding="utf-8",
    )

    merger = VacancyMerger(str(data_dir))

    json_files = merger.find_json_files()
    assert len(json_files) == 2

    merged = merger.load_and_merge_files(json_files)
    assert merger.stats["total_files_processed"] == 2
    assert merger.stats["total_vacancies_before"] == 4

    unique = merger.remove_duplicates(merged)
    assert len(unique) == 3
    assert merger.stats["duplicates_removed"] == 1
    assert merger.stats["total_vacancies_after"] == 3

    merger.analyze_vacancies(unique)
    assert merger.stats["regions_count"] == 2
    assert merger.stats["salary_stats"]["min"] == 50000
    assert merger.stats["salary_stats"]["max"] == 110000

    report_path = merger.generate_report(output_file="test_report.md")
    assert Path(report_path).exists()
    assert "ОСНОВНАЯ СТАТИСТИКА" in Path(report_path).read_text(encoding="utf-8")

