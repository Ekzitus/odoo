import argparse
import asyncio
from io import BytesIO
from PIL import Image
import logging
import random
import json
import base64
import aiohttp
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from abc import ABC, abstractmethod


async def main():
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
        filename=args.config if args.config else 'config.json', logger=logger)

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
                            odoo_password, logger)
                swapi = SWAPI(SWAPI_url, logger)
                swimg = SWIMG(SWIMG_api_url, logger, session)

                # Создаем экземпляр DataProcessor для обработки данных
                dataProc = DataProcessor(
                    swapi, swimg, odoo, logger, asyncio.get_event_loop())
                
                # Запускаем обработку данных
                await dataProc.process_data()

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
    def __init__(self, api_url, logger):
        self.api_url = api_url
        self.logger = logger

    async def fetch_data(self, session, url):
        # Асинхронный HTTP-запрос для получения данных от SWAPI
        async with session.get(url) as response:
            return await response.json()

    async def get_data(self, resource):
        url = f'{self.api_url}{resource}'
        if '/' in resource:
            # Если ресурс содержит '/', то это конечный ресурс
            async with aiohttp.ClientSession() as session:
                data = await self.fetch_data(session, url)
                self.logger.info(
                    "SWAPI: Successfully fetched data from %s", url)
                return data
        else:
            # Иначе, это ресурс с пагинацией (несколько страниц)
            i = 1
            result = []

            async with aiohttp.ClientSession() as session:
                while True:
                    param = f"page={i}"  # Параметр запроса
                    # Формирование URL с параметрами
                    url_with_params = f'{url}?{param}'
                    json_data = await self.fetch_data(session, url_with_params)
                    self.logger.info(
                        "SWAPI: Successfully fetched data from %s", url_with_params)
                    result.extend(json_data.get('results', []))

                    i += 1

                    if not json_data.get('next'):
                        break

            self.logger.info("SWAPI: Successfully fetched data from %s", url)
            return result

# Источник данных SWIMG для изображений
class SWIMG(DataSource):
    def __init__(self, api_url, logger, session):
        self.api_url = api_url
        self.logger = logger
        self.session = session

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
        async with session.get(url) as response:
            return await response.read()

    async def get_data(self, id):
        url = f'{self.api_url}{id}.jpg'

        img_data = await self.fetch(self.session, url)
        if self.is_valid_image(img_data):
            self.logger.info("Valid image")
        else:
            self.logger.info("Not a valid image")
        self.logger.info("SWIMG: Successfully fetched image data from %s", url)
        return img_data


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

    def __init__(self, url, db, username, password, logger):
        self.url = url + "/jsonrpc"
        self.db = db
        self.username = username
        self.password = password
        self.logger = logger
        self.semaphore = asyncio.Semaphore(20)

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
        async with aiohttp.ClientSession() as session:
            async with self.semaphore:
                async with session.post(url, json=data, headers={"Content-Type": "application/json"}) as response:
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
            result = await self.call(self.url, "object", "execute", self.db, uid, self.password, model, 'create', params)
            self.logger.info(f"Odoo: Successfully create on model {model}")
            return result
        except Exception as e:
            self.logger.error("Odoo: Create failed - %s", str(e))
            raise

    async def create_batch(self, uid, model, params_list):
        try:
            # Собираем список запросов на создание записей
            create_requests = [
                self.call(self.url, "object", "execute", self.db,
                          uid, self.password, model, 'create', params)
                for params in params_list
            ]

            # Выполняем запросы параллельно и получаем результаты
            results = await asyncio.gather(*create_requests)

            self.logger.info(
                f"Odoo: Successfully created {len(params_list)} records in model {model}")
            return results
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

    async def process_data(self):

        planet_swapi_to_odoo = {}

        # Используем tqdm для отображения прогресса
        with logging_redirect_tqdm():
            # Получаем данные о планетах от SWAPI
            planets = await self.swapi.get_data('planets')
            # Получаем идентификатор пользователя в Odoo
            self.uid = await self.odoo.conn()
            # Cписок для хранения параметров создания записей в res.planets
            for planet in tqdm(planets, position=0, desc='Processing Planets'):
                swapi_planet_id = planet.get('url').rstrip('/').split('/')[-1]
                name = planet.get('name')
                diameter = planet.get('diameter')
                population = planet.get('population')
                rotation_period = planet.get('rotation_period')
                orbital_period = planet.get('orbital_period')
                params = [{
                    'name': name,
                    'diameter': diameter if diameter != 'unknown' else 0,
                    'population': population if population != 'unknown' else 0,
                    'rotation_period': rotation_period if rotation_period != 'unknown' else 0,
                    'orbital_period': orbital_period if orbital_period != 'unknown' else 0,
                }]

                model = "res.planet"

                # Создаем запись в Odoo и сохраняем идентификатор в словаре
                planet_swapi_to_odoo[swapi_planet_id] = await self.odoo.create(self.uid, model, params)

                # Проверка успешного создания записи
                if planet_swapi_to_odoo[swapi_planet_id]:
                    created_id_odoo = planet_swapi_to_odoo[swapi_planet_id][0]
                    self.logger.info(
                        f"Entity: {model}, Name: {name}, ID SWAPI: {swapi_planet_id}, ID Odoo: {created_id_odoo}")
                else:
                    self.logger.error(
                        f"Error creating entity: {model}, Name: {name}, ID SWAPI: {swapi_planet_id}")

            # Получаем данные о персонажах от SWAPI
            people = await self.swapi.get_data('people')

            for person in tqdm(people, position=1, desc='Processing People'):
                id_planet_swapi = person.get(
                    'homeworld').rstrip('/').split('/')[-1]
                id_people = person.get('url').rstrip('/').split('/')[-1]
                name = person.get('name')
                id_planet_odoo = planet_swapi_to_odoo[id_planet_swapi][0]

                try:
                    # Получаем изображение для персонажа от SWIMG
                    img_data = await self.swimg.get_data(id_people)
                    img_base64 = base64.b64encode(img_data).decode("utf-8")
                except Exception as e:
                    # Обработка ошибки при получении изображения
                    self.logger.error(
                        f"Error fetching image data for person {name}, id:{id_people}: {str(e)}")
                    img_base64 = None  # Устанавливаем значение по умолчанию

                params = [{
                    'name': name,
                    'image_1920': img_base64,
                    'planet': id_planet_odoo,
                }]
                # Создание записи в Odoo
                created_ids = await self.odoo.create(self.uid, "res.partner", params)

                # Проверка успешного создания записи
                if created_ids:
                    created_id_odoo = created_ids[0]
                    self.logger.info(
                        f"Entity: res.partner, Name: {name}, ID SWAPI: {id_people}, ID Odoo: {created_id_odoo}")
                else:
                    self.logger.error(
                        f"Error creating entity: res.partner, Name: {name}, ID SWAPI: {id_people}")


if __name__ == '__main__':
    asyncio.run(main())