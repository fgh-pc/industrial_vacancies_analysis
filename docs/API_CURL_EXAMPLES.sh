#!/bin/bash
# Примеры запросов к API HeadHunter через cURL

# Базовая конфигурация
BASE_URL="https://api.hh.ru/vacancies"
USER_AGENT="UltraIndustrialCollector/9.0 (pavelkondrov03@mail.ru)"
TOKEN="APPLJ0H09NIHO3LMSSNUURRFQVEG9IK6I6KHO8E7H5DVDIVVQQC008UIGHOAUCRV"

# Функция для выполнения запроса
make_request() {
    local params="$1"
    local description="$2"
    
    echo "=========================================="
    echo "$description"
    echo "=========================================="
    echo ""
    
    curl -X GET "$BASE_URL?$params" \
        -H "User-Agent: $USER_AGENT" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Accept: application/json" \
        | jq '.' 2>/dev/null || curl -X GET "$BASE_URL?$params" \
        -H "User-Agent: $USER_AGENT" \
        -H "Authorization: Bearer $TOKEN" \
        -H "Accept: application/json"
    
    echo ""
    echo ""
}

# Пример 1: Поиск вакансий инженера в Москве
make_request \
    "professional_role=159&area=1&per_page=20&page=0&order_by=publication_time" \
    "Пример 1: Вакансии инженера в Москве (первая страница, 20 результатов)"

# Пример 2: Поиск вакансий рабочего в Санкт-Петербурге
make_request \
    "professional_role=96&area=2&per_page=20&page=0" \
    "Пример 2: Вакансии рабочего в Санкт-Петербурге"

# Пример 3: Поиск вакансий технолога за последнюю неделю
DATE_FROM=$(date -d '7 days ago' +%Y-%m-%d 2>/dev/null || date -v-7d +%Y-%m-%d 2>/dev/null || echo "2025-01-06")
make_request \
    "professional_role=156&per_page=20&page=0&date_from=$DATE_FROM" \
    "Пример 3: Вакансии технолога за последнюю неделю"

# Пример 4: Поиск по тексту "сварщик"
make_request \
    "text=сварщик&per_page=20&page=0" \
    "Пример 4: Поиск вакансий по тексту 'сварщик'"

# Пример 5: Получение информации о структуре ответа
echo "=========================================="
echo "Информация о структуре ответа API"
echo "=========================================="
echo ""
echo "Ответ API содержит следующие поля:"
echo "- items: массив вакансий (до per_page штук)"
echo "- found: общее количество найденных вакансий"
echo "- pages: количество страниц результатов"
echo "- per_page: количество результатов на странице"
echo "- page: текущая страница"
echo ""
echo "Важные ограничения:"
echo "- Максимум 2000 вакансий за один запрос (20 страниц при per_page=100)"
echo "- Лимит: 120 запросов в минуту"
echo "- Доступны только вакансии за последние 30 дней"
echo ""

