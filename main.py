import argparse
import asyncio
import logging
import random
import xmlrpc.client
import urllib.request
import json
import base64
import aiohttp
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from abc import ABC, abstractmethod

async def main():
    # Конфигурируем логирование
    logging.basicConfig(level=logging.INFO)  # Устанавливаем уровень логирования INFO

    logger = logging.getLogger(__name__)

    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Process configuration file.')
    
    # Добавляем аргумент для указания пути до файла с конфигурацией
    parser.add_argument('--config', type=str, help='Path to the configuration file')

    # Получаем аргументы
    args = parser.parse_args()

    # Создаем экземпляр Configuration, передавая путь до файла через аргумент
    config = Configuration(filename=args.config if args.config else 'config.json', logger=logger)

    if config.read_config():

        odoo_config = config.get_system_config('odoo')
        SWAPI_config = config.get_system_config('SWAPI')
        SWIMG_config = config.get_system_config('SWIMG')

        if SWAPI_config and odoo_config and SWIMG_config:
            odoo_url = odoo_config.get('url')
            odoo_db = odoo_config.get('db')
            odoo_username = odoo_config.get('username')
            odoo_password = odoo_config.get('password')

            SWAPI_url = SWAPI_config.get('url')
            SWIMG_api_url = SWIMG_config.get('url')

            async with aiohttp.ClientSession() as session:
                odoo = Odoo(odoo_url, odoo_db, odoo_username, odoo_password, logger)
                swapi = SWAPI(SWAPI_url, logger)
                swimg = SWIMG(SWIMG_api_url, logger, session)

                dataProc = DataProcessor(swapi, swimg, odoo, logger, asyncio.get_event_loop())
                await dataProc.process_data()

class DataSource(ABC):
    @abstractmethod
    def get_data(self, *args):
        pass

class SWAPI(DataSource):
    def __init__(self, api_url, logger):
        self.api_url = api_url
        self.logger = logger


    def get_data(self, resource):
        url = f'{self.api_url}{resource}'
        if '/' in resource:
            with urllib.request.urlopen(url) as response:
                data = response.read().decode('utf-8')
                self.logger.info("SWAPI: Successfully fetched data from %s", url)
                return json.loads(data)
        else:
            i = 1
            result = []

            while True:
                params = {'page': i}  # Параметры запроса
                # Формирование URL с параметрами
                url_with_params = f'{url}?{urllib.parse.urlencode(params)}'
                # Отправка GET-запроса
                with urllib.request.urlopen(url_with_params) as response:
                    data = response.read().decode('utf-8')
                    json_data = json.loads(data)
                    result.extend(json_data.get('results', []))
                
                i += 1

                if not json_data.get('next'):
                    break
            
            self.logger.info("SWAPI: Successfully fetched data from %s", url)
            return result

class SWIMG(DataSource):
    def __init__(self, api_url, logger, session):
        self.api_url = api_url
        self.logger = logger
        self.session = session

    @staticmethod
    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.read()

    async def get_data(self, id):
        url = f'{self.api_url}{id}.jpg'
        img_data = await self.fetch(self.session, url)
        self.logger.info("SWIMG: Successfully fetched image data from %s", url)
        return img_data

class Configuration:
    def __init__(self, logger, filename='config.json'):
        self.filename = filename
        self.data = {}
        self.logger = logger
    
    def read_config(self):
        try:
            with open(self.filename, 'r') as file:
                self.data = json.load(file)
            self.logger.info("Configuration: Successfully read configuration from %s", self.filename)
            return True
        except (FileNotFoundError, json.JSONDecodeError):
            self.logger.error("Configuration: Failed to read configuration from %s", self.filename)
            return False
        
    def get_system_config(self, system_name):
        return self.data.get(system_name, {})

class Odoo:

    def __init__(self, url, db, username, password, logger):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.logger = logger

    def conn(self):
        # Подключение к серверу Odoo через XML-RPC
        common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common')
        try:
            uid = common.authenticate(self.db, self.username, self.password, {})
            self.logger.info("Odoo: Successfully authenticated")
            return uid
        except Exception as e:
            self.logger.error("Odoo: Authentication failed - %s", str(e))
            raise

    def execute_kw(self, uid, model, method, *params):
        models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object')
        try:
            [result] = models.execute_kw(self.db, uid, self.password, model, method, params)
            self.logger.info("Odoo: Successfully executed method %s on model %s", method, model)
            return result
        except Exception as e:
            self.logger.error("Odoo: Method execution failed - %s", str(e))
            raise

    def create(self, uid, model, params):
        return self.execute_kw(uid, model, 'create', params)

    def read(self, uid):
        ids = self.execute_kw(uid, 'res.partner', 'search', [[]], {'limit': 1})
        [record] = self.execute_kw(uid, 'res.partner', 'read', [ids])
        return record

class DataProcessor:
    def __init__(self, swapi, swimg, odoo, logger, loop):
        self.swapi = swapi
        self.swimg = swimg
        self.odoo = odoo
        self.uid = self.odoo.conn()
        self.logger = logger
        self.loop = loop

    async def process_data(self):
        planet_swapi_to_odoo = {}
    
        with logging_redirect_tqdm():
            planets = self.swapi.get_data('planets')

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

                planet_swapi_to_odoo[swapi_planet_id] = await self.loop.run_in_executor(None, self.odoo.create, self.uid, model, params)
            
            people = self.swapi.get_data('people')

            for person in tqdm(people, position=1, desc='Processing People'):
                id_planet_swapi = person.get('homeworld').rstrip('/').split('/')[-1]
                id_people = person.get('url').rstrip('/').split('/')[-1]
                name = person.get('name') 
                id_planet_odoo = planet_swapi_to_odoo[id_planet_swapi]

                img_data = await self.swimg.get_data(id_people)

                img_base64 = base64.b64encode(img_data).decode("utf-8")

                params = [{
                    'name': name,
                    'image_1920': img_base64,
                    'planet': id_planet_odoo,
                }]

                model = "res.partner"

                await self.loop.run_in_executor(None, self.odoo.create, self.uid, model, params)

if __name__ == '__main__':
    asyncio.run(main())     