"""
СКРИПТ ДЛЯ СОЗДАНИЯ БАЗЫ ДАННЫХ ИЗ JSON ФАЙЛА
Создает industrial_vacancies.db из FINAL_MERGED_INDUSTRIAL_VACANCIES.json
"""

import os
import sys

# Добавляем путь к модулям (как в reload_data.py)
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from database.db_manager import IndustrialDatabaseManager


def create_database_from_json(force_recreate=False):
    """
    Создает базу данных industrial_vacancies.db из JSON файла.
    
    Args:
        force_recreate: Если True, пересоздает базу данных без запроса подтверждения
    """
    print("=" * 70)
    print("📦 СОЗДАНИЕ БАЗЫ ДАННЫХ ИЗ JSON ФАЙЛА")
    print("=" * 70)
    print()
    
    # Путь к JSON файлу
    json_file = "data/FINAL_MERGED_INDUSTRIAL_VACANCIES.json"
    
    # Проверяем наличие JSON файла
    if not os.path.exists(json_file):
        print(f"❌ ОШИБКА: Файл {json_file} не найден!")
        print(f"   Убедитесь, что файл существует в папке data/")
        return False
    
    # Показываем информацию о файле
    file_size = os.path.getsize(json_file) / (1024 * 1024)  # MB
    print(f"📁 Исходный файл: {json_file}")
    print(f"📏 Размер файла: {file_size:.2f} MB")
    print()
    
    # Создаем менеджер базы данных
    db_manager = IndustrialDatabaseManager(db_path="industrial_vacancies.db")
    
    # Подключаемся к базе данных
    print("🔌 Подключение к базе данных...")
    if not db_manager.create_connection():
        print("❌ ОШИБКА: Не удалось подключиться к базе данных")
        return False
    print("✅ Подключение установлено")
    print()
    
    # Проверяем, существует ли база данных с данными
    if os.path.exists("industrial_vacancies.db"):
        if force_recreate:
            print("🗑️  Пересоздание базы данных (force_recreate=True)...")
            cursor = db_manager.connection.cursor()
            # Удаляем все таблицы
            cursor.execute("DROP TABLE IF EXISTS skills")
            cursor.execute("DROP TABLE IF EXISTS vacancies")
            cursor.execute("DROP TABLE IF EXISTS regions")
            cursor.execute("DROP TABLE IF EXISTS industry_segments")
            cursor.execute("DROP TABLE IF EXISTS time_series")
            db_manager.connection.commit()
            print("✅ Старые таблицы удалены")
            print()
        else:
            print("⚠️  База данных уже существует")
            response = input("   Пересоздать базу данных? (да/нет): ").strip().lower()
            
            if response in ['да', 'yes', 'y', 'д']:
                print("🗑️  Удаление старых таблиц...")
                cursor = db_manager.connection.cursor()
                # Удаляем все таблицы
                cursor.execute("DROP TABLE IF EXISTS skills")
                cursor.execute("DROP TABLE IF EXISTS vacancies")
                cursor.execute("DROP TABLE IF EXISTS regions")
                cursor.execute("DROP TABLE IF EXISTS industry_segments")
                cursor.execute("DROP TABLE IF EXISTS time_series")
                db_manager.connection.commit()
                print("✅ Старые таблицы удалены")
                print()
            else:
                print("ℹ️  Используем существующую базу данных")
                print()
    
    # Создаем таблицы
    print("📝 Создание таблиц в базе данных...")
    if not db_manager.create_tables():
        print("❌ ОШИБКА: Не удалось создать таблицы")
        db_manager.close_connection()
        return False
    print("✅ Таблицы созданы")
    print()
    
    # Загружаем данные из JSON
    print(f"📥 Загрузка данных из {json_file}...")
    print("   Это может занять некоторое время в зависимости от размера файла...")
    print()
    
    inserted_count = db_manager.load_industrial_data_from_json(json_file)
    
    if inserted_count == 0:
        print("❌ ОШИБКА: Не удалось загрузить данные в базу")
        db_manager.close_connection()
        return False
    
    print()
    print("=" * 70)
    print("✅ БАЗА ДАННЫХ УСПЕШНО СОЗДАНА!")
    print("=" * 70)
    print()
    
    # Получаем статистику
    stats = db_manager.get_database_stats()
    
    print("📊 СТАТИСТИКА ЗАГРУЖЕННЫХ ДАННЫХ:")
    print("-" * 70)
    print(f"   • Всего вакансий: {stats.get('total_vacancies', 0):,}")
    print(f"   • С указанной зарплатой: {stats.get('vacancies_with_salary', 0):,}")
    print(f"   • Уникальных работодателей: {stats.get('unique_employers', 0):,}")
    print(f"   • Регионов: {stats.get('unique_regions', 0)}")
    print(f"   • Уникальных навыков: {stats.get('unique_skills', 0):,}")
    print()
    
    # Распределение по сегментам
    segments = stats.get('industry_segments', {})
    if segments:
        print("🏭 РАСПРЕДЕЛЕНИЕ ПО ОТРАСЛЕВЫМ СЕГМЕНТАМ:")
        print("-" * 70)
        total = stats.get('total_vacancies', 1)
        for i, (segment, count) in enumerate(list(segments.items())[:10], 1):
            percentage = (count / total) * 100 if total > 0 else 0
            print(f"   {i:2d}. {segment}: {count:,} ({percentage:.1f}%)")
        print()
    
    # Распределение по уровням позиций
    levels = stats.get('position_levels', {})
    if levels:
        print("👔 РАСПРЕДЕЛЕНИЕ ПО УРОВНЯМ ПОЗИЦИЙ:")
        print("-" * 70)
        for i, (level, count) in enumerate(list(levels.items())[:5], 1):
            percentage = (count / total) * 100 if total > 0 else 0
            print(f"   {i}. {level}: {count:,} ({percentage:.1f}%)")
        print()
    
    # Информация о базе данных
    db_size = os.path.getsize("industrial_vacancies.db") / (1024 * 1024)  # MB
    print("💾 ИНФОРМАЦИЯ О БАЗЕ ДАННЫХ:")
    print("-" * 70)
    print(f"   • Имя файла: industrial_vacancies.db")
    print(f"   • Размер файла: {db_size:.2f} MB")
    print(f"   • Загружено записей: {inserted_count:,}")
    print()
    
    print("🎯 БАЗА ДАННЫХ ГОТОВА К ИСПОЛЬЗОВАНИЮ!")
    print("   Теперь вы можете запустить comprehensive_analysis.py")
    print("=" * 70)
    
    # Закрываем соединение
    db_manager.close_connection()
    
    return True


if __name__ == "__main__":
    try:
        # Можно передать force_recreate=True для автоматического пересоздания
        # Например: python create_database.py --force
        force_recreate = '--force' in sys.argv or '-f' in sys.argv
        success = create_database_from_json(force_recreate=force_recreate)
        if success:
            print("\n✅ Процесс завершен успешно!")
            sys.exit(0)
        else:
            print("\n❌ Процесс завершился с ошибками")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Процесс прерван пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

