import json
from collections import Counter

def filter_industrial_vacancies():
    # Загружаем данные
    with open('data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json', 'r', encoding='utf-8') as f:
        vacancies = json.load(f)
    
    print(f"Всего вакансий до фильтрации: {len(vacancies)}")
    
    # Ключевые слова для исключения непромышленных вакансий
    exclude_keywords = [
        # IT и офисные профессии
        'devops', 'разработчик', 'программист', 'developer', 'engineer', 
        'data scientist', 'аналитик', 'analyst', 'дизайнер', 'designer',
        'маркетолог', 'менеджер по продажам', 'менеджер проектов',
        'рекрутер', 'hr', 'кадры', 'бухгалтер', 'экономист', 'финансист',
        'юрист', 'адвокат', 'юрисконсульт', 'консультант',
        'администратор', 'секретарь', 'офис-менеджер',
        'копирайтер', 'контент-менеджер', 'smm', 'таргетолог',
        
        # Кол-центры и call-центры (исключаем только офисных операторов)
        'оператор кол-центра', 'оператор колл-центра', 'оператор call-центра',
        'оператор контакт-центра', 'специалист кол-центра', 
        'специалист колл-центра', 'специалист call-центра',
        'менеджер кол-центра', 'менеджер колл-центра', 'менеджер call-центра',
        'агент кол-центра', 'агент колл-центра', 'агент call-центра',
        'кол-центр', 'колл-центр', 'call-центр', 'контакт-центр',
        'телефонный оператор', 'оператор горячей линии',
        'оператор пк', 'оператор 1с', 'оператор базы данных',
        
        # Медицина и фармацевтика
        'врач', 'медицинск', 'фельдшер', 'медсестра', 'медбрат',
        'стоматолог', 'хирург', 'терапевт', 'педиатр', 'фармацевт',
        'провизор', 'лаборант', 'рентген', 'узи',
        
        # Образование
        'преподаватель', 'учитель', 'тренер', 'педагог', 'репетитор',
        'методист', 'воспитатель',
        
        # Торговля и услуги
        'продавец', 'кассир', 'мерчандайзер', 'консультант',
        'бариста', 'официант', 'повар', 'бармен', 'сушист',
        'парикмахер', 'визажист', 'косметолог', 'массажист',
        'водитель', 'курьер', 'логист', 'экспедитор',
        
        # Другие непромышленные
        'агент', 'риелтор', 'брокер', 'страховой',
        'журналист', 'редактор', 'корреспондент',
        'психолог', 'социальный работник'
    ]
    
    # Ключевые слова для производственных операторов (оставляем)
    production_operator_keywords = [
        'оператор линии', 'оператор производств', 'оператор станк',
        'оператор чпу', 'оператор оборудован', 'оператор установк',
        'оператор аппарат', 'оператор машины', 'оператор агрегат',
        'оператор технологич', 'оператор цех', 'оператор завод',
        'оператор фабрик', 'машинист', 'аппаратчик'
    ]
    
    # Фильтруем вакансии
    industrial_vacancies = []
    removed_vacancies = []
    
    for vacancy in vacancies:
        name = vacancy.get('name', '').lower()
        
        # Проверяем, является ли оператор производственным
        is_production_operator = any(keyword in name for keyword in production_operator_keywords)
        
        # Проверяем, содержит ли название исключающие ключевые слова
        should_exclude = any(keyword in name for keyword in exclude_keywords)
        
        # Если это производственный оператор - не исключаем, даже если есть слово "оператор"
        if is_production_operator:
            industrial_vacancies.append(vacancy)
        elif not should_exclude:
            industrial_vacancies.append(vacancy)
        else:
            removed_vacancies.append(vacancy)
    
    print(f"Промышленных вакансий после фильтрации: {len(industrial_vacancies)}")
    print(f"Удалено непромышленных вакансий: {len(removed_vacancies)}")
    
    # Сохраняем отфильтрованные данные
    with open('data/FILTERED_INDUSTRIAL_VACANCIES.json', 'w', encoding='utf-8') as f:
        json.dump(industrial_vacancies, f, ensure_ascii=False, indent=2)
    
    # Создаем отчет по отфильтрованным вакансиям
    names = [v.get('name', 'Без названия') for v in industrial_vacancies]
    count_by_name = Counter(names)
    
    print("\nТОП-20 ПРОМЫШЛЕННЫХ ВАКАНСИЙ:")
    print("-" * 50)
    for name, count in count_by_name.most_common(20):
        print(f"{name} - {count} вакансий")
    
    # Сохраняем также список удаленных вакансий для проверки
    with open('data/REMOVED_NON_INDUSTRIAL_VACANCIES.json', 'w', encoding='utf-8') as f:
        json.dump(removed_vacancies, f, ensure_ascii=False, indent=2)
    
    return industrial_vacancies

# Улучшенная версия с лучшей фильтрацией операторов
def smart_industrial_filter():
    with open('data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json', 'r', encoding='utf-8') as f:
        vacancies = json.load(f)
    
    print(f"Всего вакансий до фильтрации: {len(vacancies)}")
    
    # Ключевые слова для промышленных профессий (включаем)
    industrial_keywords = [
        'инженер', 'техник', 'механик', 'электрик', 'сварщик', 'токарь',
        'фрезеровщик', 'слесарь', 'монтажник', 'наладчик', 'машинист',
        'технолог', 'конструктор', 'проектировщик', 'оборудован', 
        'производств', 'цех', 'завод', 'фабрика', 'строитель', 
        'монтаж', 'ремонт', 'обслуживан', 'эксплуатац', 'энергетик',
        'нефть', 'газ', 'хими', 'металл', 'горн', 'бурильщик', 
        'геолог', 'обогатитель', 'обогащен'
    ]
    
    # Производственные операторы (включаем)
    production_operators = [
        'оператор линии', 'оператор производств', 'оператор станк',
        'оператор чпу', 'оператор оборудован', 'оператор установк',
        'оператор аппарат', 'оператор машины', 'оператор агрегат',
        'оператор технологич', 'оператор цех', 'оператор завод',
        'оператор фабрик', 'машинист', 'аппаратчик'
    ]
    
    # Офисные операторы (исключаем)
    office_operators = [
        'оператор кол-центра', 'оператор колл-центра', 'оператор call-центра',
        'оператор контакт-центра', 'оператор пк', 'оператор 1с', 
        'оператор базы данных', 'оператор горячей линии',
        'телефонный оператор'
    ]
    
    # Другие исключения
    other_exclude = [
        'devops', 'разработчик', 'программист', 'it', 'айти',
        'бухгалтер', 'экономист', 'финанс', 'юрист', 'адвокат',
        'менеджер по продажам', 'маркетолог', 'рекрутер', 'hr',
        'врач', 'медицинск', 'фельдшер', 'медсестра', 'стоматолог',
        'преподаватель', 'учитель', 'тренер', 'педагог',
        'продавец', 'кассир', 'консультант', 'мерчандайзер',
        'водитель', 'курьер', 'логист', 'экспедитор',
        'повар', 'официант', 'бариста', 'бармен'
    ]
    
    industrial_vacancies = []
    
    for vacancy in vacancies:
        name = vacancy.get('name', '').lower()
        
        # Проверяем профессиональные роли
        professional_roles = vacancy.get('professional_roles', [])
        roles_text = ' '.join([role.get('name', '').lower() for role in professional_roles])
        
        # Определяем тип вакансии
        is_production_operator = any(keyword in name for keyword in production_operators)
        is_office_operator = any(keyword in name for keyword in office_operators)
        is_industrial = any(keyword in name for keyword in industrial_keywords) or \
                       any(keyword in roles_text for keyword in industrial_keywords)
        is_excluded = any(keyword in name for keyword in other_exclude)
        
        # Логика фильтрации:
        # 1. Производственные операторы - включаем
        # 2. Офисные операторы - исключаем
        # 3. Другие промышленные - включаем
        # 4. Исключенные - исключаем
        if is_production_operator:
            industrial_vacancies.append(vacancy)
        elif is_office_operator:
            continue  # исключаем
        elif is_industrial and not is_excluded:
            industrial_vacancies.append(vacancy)
    
    print(f"Промышленных вакансий после умной фильтрации: {len(industrial_vacancies)}")
    
    # Сохраняем результат
    with open('data/SMART_FILTERED_INDUSTRIAL_VACANCIES.json', 'w', encoding='utf-8') as f:
        json.dump(industrial_vacancies, f, ensure_ascii=False, indent=2)
    
    # Отчет
    names = [v.get('name', 'Без названия') for v in industrial_vacancies]
    count_by_name = Counter(names)
    
    print("\nТОП-20 ПРОМЫШЛЕННЫХ ВАКАНСИЙ:")
    print("-" * 50)
    for name, count in count_by_name.most_common(20):
        print(f"{name} - {count} вакансий")
    
    return industrial_vacancies

# Функция для проверки операторов
def check_operators_after_filter():
    with open('data/SMART_FILTERED_INDUSTRIAL_VACANCIES.json', 'r', encoding='utf-8') as f:
        filtered = json.load(f)
    
    operator_vacancies = [v for v in filtered if 'оператор' in v.get('name', '').lower()]
    
    print(f"\nВакансии с 'оператор' после фильтрации ({len(operator_vacancies)}):")
    for vacancy in operator_vacancies[:30]:  # покажем первые 30
        print(f"  - {vacancy.get('name')}")

# Запуск фильтрации
if __name__ == "__main__":
    print("УМНАЯ ФИЛЬТРАЦИЯ (сохраняет производственных операторов):")
    industrial_vacancies = smart_industrial_filter()
    
    print("\n" + "="*80 + "\n")
    
    print("ПРОВЕРКА ОПЕРАТОРОВ:")
    check_operators_after_filter()