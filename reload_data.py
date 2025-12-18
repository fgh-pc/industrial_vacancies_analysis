# reload_data.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from database.db_manager import IndustrialDatabaseManager

def reload_all_data():
    """Перезагружает все данные с упрощенной фильтрацией."""
    print("🔄 ПЕРЕЗАГРУЗКА ДАННЫХ С УПРОЩЕННОЙ ФИЛЬТРАЦИЕЙ")
    print("=" * 60)
    
    db_manager = IndustrialDatabaseManager()
    
    if db_manager.create_connection():
        # Удаляем старые таблицы
        print("🗑️ Удаляем старые данные...")
        cursor = db_manager.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS skills")
        cursor.execute("DROP TABLE IF EXISTS vacancies")
        cursor.execute("DROP TABLE IF EXISTS regions")
        cursor.execute("DROP TABLE IF EXISTS industry_segments")
        cursor.execute("DROP TABLE IF EXISTS time_series")
        db_manager.connection.commit()
        
        # Создаем новые таблицы
        print("📝 Создаем таблицы...")
        db_manager.create_tables()
        
        # Загружаем данные с упрощенной фильтрацией
        json_file = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
        print(f"📥 Загружаем данные из {json_file}...")
        
        inserted = db_manager.load_industrial_data_from_json(json_file)
        
        if inserted > 0:
            stats = db_manager.get_database_stats()
            print(f"\n✅ ПЕРЕЗАГРУЗКА ЗАВЕРШЕНА:")
            print(f"   📊 Загружено вакансий: {stats.get('total_vacancies', 0):,}")
            print(f"   💰 С зарплатой: {stats.get('vacancies_with_salary', 0):,}")
            print(f"   🏢 Работодателей: {stats.get('unique_employers', 0):,}")
            print(f"   🌍 Регионов: {stats.get('unique_regions', 0):,}")
            
            # Показываем распределение по сегментам
            print(f"\n🏭 РАСПРЕДЕЛЕНИЕ ПО СЕГМЕНТАМ:")
            segments = stats.get('industry_segments', {})
            for segment, count in list(segments.items())[:10]:
                percentage = (count / stats['total_vacancies']) * 100
                print(f"   {segment}: {count:,} ({percentage:.1f}%)")
        else:
            print("❌ Не удалось загрузить данные")
        
        db_manager.close_connection()
    else:
        print("❌ Не удалось подключиться к базе данных")

if __name__ == "__main__":
    reload_all_data()