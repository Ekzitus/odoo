import asyncio
import aiohttp


async def fetch_planet_info(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    # Обработка ошибки 404 (ресурс не найден)
                    print(f"Error fetching planet info from {url}: Planet not found")
                    return None
                else:
                    print(f"Error fetching planet info from {url}: Status {response.status}")
                    return None
    except aiohttp.ClientError as e:
        print(f"Error fetching planet info from {url}: {e}")
        return None
async def main():
    # Пример использования:
    url = "https://swapi.dev/api/planets/41"
    planet_info = await fetch_planet_info(url)
    if planet_info:
        print("Planet info:", planet_info)
    else:
        print("Failed to fetch planet info")

if __name__ == '__main__':
    asyncio.run(main())