-- Создание таблицы для вакансий
CREATE TABLE IF NOT EXISTS vacancies (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    area TEXT,
    salary_from INTEGER,
    salary_to INTEGER,
    salary_currency TEXT,
    experience TEXT,
    schedule TEXT,
    employment TEXT,
    published_at TEXT,
    employer_name TEXT,
    snippet_requirement TEXT,
    snippet_responsibility TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы для навыков
CREATE TABLE IF NOT EXISTS skills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vacancy_id INTEGER,
    skill_name TEXT,
    FOREIGN KEY (vacancy_id) REFERENCES vacancies (id)
);

-- Индексы для ускорения поиска
CREATE INDEX IF NOT EXISTS idx_vacancies_area ON vacancies(area);
CREATE INDEX IF NOT EXISTS idx_vacancies_experience ON vacancies(experience);
CREATE INDEX IF NOT EXISTS idx_vacancies_published_at ON vacancies(published_at);
CREATE INDEX IF NOT EXISTS idx_skills_vacancy_id ON skills(vacancy_id);
CREATE INDEX IF NOT EXISTS idx_vacancies_salary_from ON vacancies(salary_from);
CREATE INDEX IF NOT EXISTS idx_vacancies_salary_to ON vacancies(salary_to);

SELECT 'Таблицы и индексы успешно созданы!' as status;
