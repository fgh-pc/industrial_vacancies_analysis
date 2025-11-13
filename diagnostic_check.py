# diagnostic_check.py
import json
import os
import sys

# Добавляем путь к src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from database.db_manager import IndustrialDatabaseManager
except ImportError:
    print("❌ Не удалось импортировать db_manager")
    # Создаем простую версию для диагностики
    class SimpleDBManager:
        def create_connection(self):
            try:
                import sqlite3
                self.connection = sqlite3.connect("industrial_vacancies.db")
                return True
            except:
                return False
        
        def close_connection(self):
            if hasattr(self, 'connection'):
                self.connection.close()

def check_data_issues():
    """Быстрая проверка проблем с данными."""
    
    # Проверяем JSON файл
    json_file = 'data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json'
    if not os.path.exists(json_file):
        print(f"❌ Файл {json_file} не найден")
        return
    
    print("🔍 ДИАГНОСТИКА ДАННЫХ")
    print("=" * 50)
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"📁 В JSON файле: {len(data):,} вакансий")
    
    # Анализируем JSON данные
    print("\n📊 АНАЛИЗ JSON ДАННЫХ:")
    
    # Проверяем уникальность ID
    ids = [v.get('id') for v in data if v.get('id')]
    unique_ids = set(ids)
    print(f"  🔑 Уникальных ID: {len(unique_ids):,} из {len(ids):,}")
    
    # Проверяем наличие зарплат
    salaries_count = sum(1 for v in data if v.get('salary'))
    print(f"  💰 С зарплатой: {salaries_count:,} ({salaries_count/len(data)*100:.1f}%)")
    
    # Проверяем регионы
    regions = set()
    for v in data[:1000]:  # Проверяем выборку для скорости
        area = v.get('area', {})
        if isinstance(area, dict) and area.get('name'):
            regions.add(area['name'])
    print(f"  🌍 Регионов (выборка): {len(regions)}")
    
    # Проверяем базу данных
    print("\n💾 ПРОВЕРКА БАЗЫ ДАННЫХ:")
    
    try:
        db_manager = IndustrialDatabaseManager()
        if db_manager.create_connection():
            cursor = db_manager.connection.cursor()
            
            # Общее количество
            cursor.execute("SELECT COUNT(*) FROM vacancies")
            db_count = cursor.fetchone()[0]
            print(f"  Всего вакансий в БД: {db_count:,}")
            
            # Промышленные вакансии
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE is_industrial = 1")
            industrial_count = cursor.fetchone()[0]
            print(f"  Промышленных вакансий: {industrial_count:,}")
            
            # Вакансии с зарплатой
            cursor.execute("SELECT COUNT(*) FROM vacancies WHERE has_salary = 1")
            with_salary_count = cursor.fetchone()[0]
            print(f"  С зарплатой в БД: {with_salary_count:,}")
            
            # Уникальные работодатели
            cursor.execute("SELECT COUNT(DISTINCT employer_name) FROM vacancies")
            employers_count = cursor.fetchone()[0]
            print(f"  Уникальных работодателей: {employers_count:,}")
            
            db_manager.close_connection()
            
            # Анализируем разницу
            print(f"\n📈 АНАЛИЗ РАСХОЖДЕНИЙ:")
            difference = len(data) - db_count
            print(f"  Не загружено в БД: {difference:,} вакансий")
            
            if difference > 0:
                success_rate = (db_count / len(data)) * 100
                print(f"  Процент загрузки: {success_rate:.1f}%")
                
                if success_rate < 50:
                    print("\n⚠️  ВОЗМОЖНЫЕ ПРИЧИНЫ НИЗКОЙ ЗАГРУЗКИ:")
                    print("  • Строгая фильтрация промышленных вакансий")
                    print("  • Много дубликатов в исходных данных")
                    print("  • Ошибки формата в некоторых вакансиях")
                    print("  • Проблемы с обработкой специальных символов")
                    
        else:
            print("  ❌ Не удалось подключиться к базе данных")
            
    except Exception as e:
        print(f"  ❌ Ошибка при проверке БД: {e}")

def check_industrial_filter():
    """Проверяет сколько вакансий проходит фильтрацию."""
    print("\n🔧 ПРОВЕРКА ФИЛЬТРАЦИИ ПРОМЫШЛЕННЫХ ВАКАНСИЙ:")
    
    json_file = 'data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json'
    if not os.path.exists(json_file):
        return
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Простая проверка промышленных ключевых слов
    industrial_keywords = [
        'инженер', 'технолог', 'конструктор', 'механик', 'электрик',
        'сварщик', 'токарь', 'фрезеровщик', 'наладчик', 'оператор',
        'аппаратчик', 'машинист', 'монтажник', 'ремонтник', 'станочник',
        'кип', 'кипиа', 'асутп', 'автоматизация', 'энергетик',
        'нефтяник', 'газовик', 'бурильщик', 'горняк', 'металлург'
    ]
    
    industrial_count = 0
    sample_size = min(1000, len(data))
    
    for i in range(sample_size):
        vacancy = data[i]
        name = vacancy.get('name', '').lower()
        
        for keyword in industrial_keywords:
            if keyword in name:
                industrial_count += 1
                break
    
    print(f"  Промышленные вакансии (выборка {sample_size}): {industrial_count} ({industrial_count/sample_size*100:.1f}%)")
    
    # Проверяем категорию "другое"
    try:
        db_manager = IndustrialDatabaseManager()
        if db_manager.create_connection():
            cursor = db_manager.connection.cursor()
            cursor.execute("SELECT industry_segment, COUNT(*) as count FROM vacancies GROUP BY industry_segment ORDER BY count DESC")
            segments = cursor.fetchall()
            
            print(f"\n🏭 РАСПРЕДЕЛЕНИЕ ПО СЕГМЕНТАМ В БД:")
            for segment, count in segments:
                percentage = (count / industrial_count) * 100 if industrial_count > 0 else 0
                print(f"  {segment}: {count:,} ({percentage:.1f}%)")
            
            db_manager.close_connection()
    except:
        pass

if __name__ == "__main__":
    check_data_issues()
    check_industrial_filter()
    print("\n" + "=" * 50)
    print("✅ ДИАГНОСТИКА ЗАВЕРШЕНА")