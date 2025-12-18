# Скрипт для инициализации Git репозитория в правильной директории
$projectPath = "C:\Users\1\OneDrive\Рабочий стол\Git Hub\industrial_vacancies_analysis"

# Переходим в директорию проекта
Set-Location -LiteralPath $projectPath

# Проверяем, что мы в правильной директории
if (Test-Path "vacancy_merger.py" -and Test-Path "create_database.py") {
    Write-Host "✓ Находимся в правильной директории проекта"
    
    # Инициализируем Git репозиторий
    if (Test-Path ".git") {
        Write-Host "⚠ Git репозиторий уже инициализирован"
    } else {
        git init
        Write-Host "✓ Git репозиторий инициализирован"
    }
    
    # Показываем статус
    Write-Host "`nПроверка исключений из .gitignore:"
    git check-ignore -v update_dates_for_dynamics.py PRESENTATION_CRITERIA.md PERFORMANCE_CHARACTERISTICS.md database_model.png archive/ 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Указанные файлы правильно исключены"
    }
    
    Write-Host "`nГотово! Теперь вы можете выполнить:"
    Write-Host "  git add ."
    Write-Host "  git commit -m 'Initial commit'"
    Write-Host "  git remote add origin <your-repo-url>"
    Write-Host "  git push -u origin main"
} else {
    Write-Host "✗ Ошибка: не найдены файлы проекта. Проверьте путь."
}

