# Простой запуск сбора
from src.api.hh_api_client import HHAPIClient

async def collect_data():
    client = HHAPIClient()
    
    vacancies = await client.collect_industrial_vacancies(
        queries=client.get_industrial_queries(),
        areas=client.get_industrial_regions(),
        days_back=365,  # За год
        max_vacancies=500000  # Целевое количество
    )
    
    await client.close()

# Запуск
import asyncio
asyncio.run(collect_data())