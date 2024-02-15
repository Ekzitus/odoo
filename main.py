import argparse
import asyncio
import logging
import random
import json
import base64
import time
from io import BytesIO
from PIL import Image
from abc import ABC, abstractmethod

import aiohttp
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


async def main():
    start_time = time.time()
    # Конфигурируем логирование
    # Устанавливаем уровень логирования INFO
    logging.basicConfig(level=logging.INFO)

    # Получаем логгер для текущего модуля
    logger = logging.getLogger(__name__)

    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Process configuration file.')

    # Добавляем аргумент для указания пути до файла с конфигурацией
    parser.add_argument('--config', type=str,
                        help='Path to the configuration file')

    # Получаем аргументы
    args = parser.parse_args()

    # Создаем экземпляр JsonConfiguration, передавая путь до файла через аргумент
    config = JsonConfiguration(
        filename=args.config if args.config else 'config copy.json', logger=logger)

    # Если удалось прочитать конфигурацию
    if config.read_config():

        # Получаем конфигурации для различных систем
        odoo_config = config.get_system_config('odoo')
        SWAPI_config = config.get_system_config('SWAPI')
        SWIMG_config = config.get_system_config('SWIMG')

        # Если все конфигурации доступны
        if SWAPI_config and odoo_config and SWIMG_config:
            odoo_url = odoo_config.get('url')
            odoo_db = odoo_config.get('db')
            odoo_username = odoo_config.get('username')
            odoo_password = odoo_config.get('password')

            SWAPI_url = SWAPI_config.get('url')
            SWIMG_api_url = SWIMG_config.get('url')

            # Используем aiohttp для асинхронных HTTP-запросов
            async with aiohttp.ClientSession() as session:
                odoo = Odoo(odoo_url, odoo_db, odoo_username,
                            odoo_password, logger, session)
                swapi = SWAPI(SWAPI_url, logger, session)
                swimg = SWIMG(SWIMG_api_url, logger, session)

                # Создаем экземпляр DataProcessor для обработки данных
                dataProc = DataProcessor(
                    swapi, swimg, odoo, logger, asyncio.get_event_loop())

                # Запускаем обработку данных
                await dataProc.process_data()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Program executed in {execution_time:.2f} seconds")


# Абстрактный класс источника данных
class DataSource(ABC):
    @abstractmethod
    def get_data(self, *args):
        pass


# Абстрактный класс конфигурации
class Configuration(ABC):
    def __init__(self, logger):
        self.data = {}
        self.logger = logger

    @abstractmethod
    def read_config(self):
        pass

    @abstractmethod
    def get_system_config(self, system_name):
        pass


# Источник данных SWAPI
class SWAPI(DataSource):
    def __init__(self, api_url, logger, session):
        self.api_url = api_url
        self.logger = logger
        self.session = session
        self.semaphore = asyncio.Semaphore(50)

    async def fetch_data(self, session, url):
        # Асинхронный HTTP-запрос для получения данных от SWAPI
        async with self.semaphore:
            async with session.get(url) as response:
                if (response.status == 200):
                    return await response.json()
                else:
                    self.logger.error(
                        f"Произошла ошибка при выполнении запроса. Url: {url} Статус: {response.status}")

    async def get_data(self, resource):
        url_res = f'{self.api_url}{resource}'
        if '/' in resource:
            # Если ресурс содержит '/', то это конечный ресурс
            data = await self.fetch_data(self.session, url_res)
            self.logger.info(
                "SWAPI: Successfully fetched data from %s", url_res)
            return data
        else:
            count = await self.get_count(resource)
            urls = [f'{url_res}/{i}' for i in range(1, count + 1)]
            tasks = [self.fetch_data(self.session, url) for url in urls]
            results = await asyncio.gather(*tasks)
            return results

    async def get_count(self, resource):
        url_res = f'{self.api_url}{resource}'
        json_data = await self.fetch_data(self.session, url_res)
        return json_data.get('count')


# Источник данных SWIMG для изображений
class SWIMG(DataSource):
    def __init__(self, api_url, logger, session):
        self.api_url = api_url
        self.logger = logger
        self.session = session
        self.semaphore = asyncio.Semaphore(50)

    def is_valid_image(self, data):
        try:
            # Проверка целостности изображения
            img = Image.open(BytesIO(data))
            img.verify()
            return True
        except Exception as e:
            self.logger.error(f"Error validating image: {e}")
            return False

    async def fetch(self, session, url):
        # Асинхронный HTTP-запрос для получения данных изображения от SWIMG
        async with self.semaphore:
            async with session.get(url) as response:
                return await response.read()

    async def get_data(self, ids):
        async def fetch_and_validate(id):
            url = f'{self.api_url}{id}.jpg'
            img_data = await self.fetch(self.session, url)
            if self.is_valid_image(img_data):
                self.logger.info(
                    "SWIMG: Successfully fetched image data from %s", url)
                return id, img_data
            else:
                return id, None

        if isinstance(ids, list):
            tasks = [fetch_and_validate(id) for id in ids]
            results = {}
            with tqdm(total=len(ids), desc="Загрузка изображений") as pbar:
                for coro in asyncio.as_completed(tasks):
                    id, result = await coro
                    results[id] = result
                    pbar.update(1)
            return [results[id] for id in ids]
        else:
            id, result = await fetch_and_validate(ids)
            return result


class JsonConfiguration(Configuration):
    def __init__(self, logger, filename='config.json'):
        self.filename = filename
        self.data = {}
        self.logger = logger

    def read_config(self):
        try:
            # Чтение конфигурации из файла JSON
            with open(self.filename, 'r') as file:
                self.data = json.load(file)
            self.logger.info(
                "Configuration: Successfully read configuration from %s", self.filename)
            return True
        except (FileNotFoundError, json.JSONDecodeError):
            self.logger.error(
                "Configuration: Failed to read configuration from %s", self.filename)
            return False

    def get_system_config(self, system_name):
        return self.data.get(system_name, {})


# Класс для взаимодействия с Odoo
class Odoo:

    def __init__(self, url, db, username, password, logger, session):
        self.url = url + "/jsonrpc"
        self.db = db
        self.username = username
        self.password = password
        self.logger = logger
        self.semaphore = asyncio.Semaphore(20)
        self.session = session

    async def conn(self):
        try:
            uid = await self.call(self.url, "common", "login", self.db, self.username, self.password)
            self.logger.info("Odoo: Successfully authenticated")
            return uid
        except Exception as e:
            self.logger.error("Odoo: Authentication failed - %s", str(e))
            raise

    async def json_rpc(self, url, method, params):
        data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": random.randint(0, 1000000000),
        }
        async with self.semaphore:
            async with self.session.post(url, json=data, headers={"Content-Type": "application/json"}) as response:
                reply = await response.json()
                if reply.get("error"):
                    raise Exception(reply["error"])
                return reply["result"]

    async def call(self, url, service, method, *args):
        # Выполнение JSON-RPC вызова
        return await self.json_rpc(url, "call", {"service": service, "method": method, "args": args})

    async def create(self, uid, model, params):
        try:
            # Создание записи в Odoo
            [result] = await self.call(self.url, "object", "execute", self.db, uid, self.password, model, 'create', params)
            self.logger.info(f"Odoo: Successfully create on model {model}")
            return result
        except Exception as e:
            self.logger.error("Odoo: Create failed - %s", str(e))
            raise

    async def create_batch(self, uid, model, params_list):
        try:
            # Создаем список для хранения завершенных фьючерсов
            completed_requests = []

            # Собираем список запросов на создание записей
            create_requests = [
                self.call(self.url, "object", "execute", self.db,
                          uid, self.password, model, 'create', params)
                for params in params_list
            ]

            # Отслеживаем прогресс с помощью прогресс-бара
            with tqdm(total=len(params_list), desc=f"Creating records in model {model}", position=0) as pbar:
                # Используем as_completed для отслеживания завершения каждого запроса
                for coro in asyncio.as_completed(create_requests):
                    # Ожидаем завешения текущего запроса
                    result = await coro
                    completed_requests.append(result)
                    # Обновляем прогресс-бар
                    pbar.update(1)

            self.logger.info(
                f"Odoo: Successfully created {len(params_list)} records in model {model}")
            return completed_requests
        except Exception as e:
            self.logger.error(f"Odoo: Batch create failed - {str(e)}")
            raise


# Класс для обработки данных
class DataProcessor:
    def __init__(self, swapi, swimg, odoo, logger, loop):
        self.swapi = swapi
        self.swimg = swimg
        self.odoo = odoo
        self.logger = logger
        self.loop = loop

    async def planet(self, id):
        swapi_planet = await self.swapi.get_data(f'planets/{id}')
        if not swapi_planet:
            return id, None
        name = swapi_planet.get('name')
        diameter = swapi_planet.get('diameter')
        population = swapi_planet.get('population')
        rotation_period = swapi_planet.get('rotation_period')
        orbital_period = swapi_planet.get('orbital_period')
        params = [{
            'name': name,
            'diameter': diameter if diameter != 'unknown' else None,
            'population': population if population != 'unknown' else None,
            'rotation_period': rotation_period if rotation_period != 'unknown' else None,
            'orbital_period': orbital_period if orbital_period != 'unknown' else None,
        }]
        return id, await self.odoo.create(self.uid, "res.planet", params)

    async def people(self, id):
        swapi_people = await self.swapi.get_data(f'people/{id}')
        if not swapi_people:
            return
        name = swapi_people.get('name')
        id_planet_swapi = swapi_people.get(
            'homeworld').rstrip('/').split('/')[-1]
        id_planet_odoo = self.planet_swapi_to_odoo[int(id_planet_swapi) - 1]
        img_data = await self.swimg.get_data(id)
        params = [{
            'name': name,
            'image_1920': base64.b64encode(img_data).decode("utf-8"),
            'planet': id_planet_odoo,
        }]
        await self.odoo.create(self.uid, "res.partner", params)

    async def process_data(self):

        planet_swapi_to_odoo = {}

        # Используем tqdm для отображения прогресса
        with logging_redirect_tqdm():
            # Получаем идентификатор пользователя в Odoo
            self.uid = await self.odoo.conn()
            swapi_resource = 'planets'
            planets_count = await self.swapi.get_count(swapi_resource)
            tasks = [self.planet(id) for id in range(1, planets_count+1)]
            planet_swapi_to_odoo = {}
            with tqdm(total=planets_count, desc="Загрузка planet") as pbar:
                for coro in asyncio.as_completed(tasks):
                    swapi_id_planet, odoo_id_planet = await coro
                    planet_swapi_to_odoo[swapi_id_planet] = odoo_id_planet
                    pbar.update(1)

            self.planet_swapi_to_odoo = [planet_swapi_to_odoo[swapi_id_planet]
                                         for swapi_id_planet in range(1, planets_count+1)]

            swapi_resource = 'people'
            people_count = await self.swapi.get_count(swapi_resource)
            tasks = [self.people(id) for id in range(1, people_count+1)]

            with tqdm(total=people_count, desc="Загрузка people") as pbar:
                for coro in asyncio.as_completed(tasks):
                    await coro
                    pbar.update(1)


if __name__ == '__main__':
    asyncio.run(main())
