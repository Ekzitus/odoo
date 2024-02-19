import argparse
import asyncio
import logging
import random
import json
import base64
from io import BytesIO
from PIL import Image
from abc import ABC, abstractmethod

import aiohttp
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


async def main():
    """
    Главная функция программы.
    Запускает основную логику программы.
    """
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    description = 'Process configuration file.'
    parser = argparse.ArgumentParser(description)
    help = 'Path to the configuration file'
    parser.add_argument('--config', type=str, help=help)
    args = parser.parse_args()

    config = JsonConfiguration(logger, args.config
                               if args.config else 'config.json')

    if config.read_config():

        odoo_config = config.get_system_config('Odoo')
        swapi_config = config.get_system_config('SWAPI')
        swimg_config = config.get_system_config('SWIMG')

        if swapi_config and odoo_config and swimg_config:
            async with aiohttp.ClientSession() as session:
                odoo = DataSourceFactory.create_data_source(
                    'Odoo', odoo_config, logger, session)
                swapi = DataSourceFactory.create_data_source(
                    'SWAPI', swapi_config, logger, session)
                swimg = DataSourceFactory.create_data_source(
                    'SWIMG', swimg_config, logger, session)

                dataProc = DataProcessor(
                    swapi, swimg, odoo, logger, asyncio.get_event_loop())

                await dataProc.process_data()


class DataSourceFactory:
    @staticmethod
    def create_data_source(system_name, config, logger, session):
        if system_name == 'SWAPI':
            return SWAPI(config.get('url'), logger, session)
        elif system_name == 'SWIMG':
            return SWIMG(config.get('url'), logger, session)
        elif system_name == 'Odoo':
            odoo_url = config.get('url')
            odoo_db = config.get('db')
            odoo_username = config.get('username')
            odoo_password = config.get('password')
            return Odoo(odoo_url, odoo_db, odoo_username, odoo_password, logger, session)
        else:
            raise ValueError(f"Unknown system name: {system_name}")


class DataSource(ABC):
    """
    Абстрактный базовый класс для источников данных.
    """
    @abstractmethod
    def get_data(self, *args):
        """
        Абстрактный метод для получения данных.
        """
        pass


class Configuration(ABC):
    """
    Абстрактный базовый класс для работы с конфигурацией.

    """

    def __init__(self, logger):
        """
        Инициализация объекта класса Configuration.

        Параметры:
            logger (logging.Logger): Логгер для записи сообщений.
        """
        self.data = {}
        self.logger = logger

    @abstractmethod
    def read_config(self):
        """
        Абстрактный метод для чтения конфигурационных данных.

        Возвращает:
            bool: Результат чтения конфигурации 
            (True - успешно, False - ошибка).
        """
        pass

    @abstractmethod
    def get_system_config(self, system_name):
        """
        Абстрактный метод для получения конфигурации 
        для указанной системы.

        Параметры:
            system_name (str): Название системы.

        Возвращает:
            dict: Конфигурационные данные для указанной системы.
        """
        pass


class SWAPI(DataSource):
    """
    Класс для работы с The Star Wars API.
    """

    def __init__(self, api_url, logger, session):
        """
        Инициализация объекта класса SWAPI.

        Параметры:
            api_url (str): URL SWAPI.
            logger (logging.Logger): Логгер для записи сообщений.
            session (aiohttp.ClientSession): Сессия для работы с API.
        """
        self.api_url = api_url
        self.logger = logger
        self.session = session
        self.semaphore = asyncio.Semaphore(50)

    async def fetch_data(self, session, url):
        """
        Получение данных по указанному URL.

        Параметры:
            session (aiohttp.ClientSession): Сессия для работы с API.
            url (str): URL для получения данных.

        Возвращает:
            dict: Полученные данные.
        """
        async with self.semaphore:
            async with session.get(url) as response:
                if (response.status == 200):
                    return await response.json()
                else:
                    self.logger.error(
                        "Произошла ошибка при выполнении запроса." +
                        f"Url: {url} Статус: {response.status}")

    async def get_data(self, resource):
        """
        Получение данных из SWAPI.

        Параметры:
            resource (str): Ресурс для запроса данных.

        Возвращает:
            dict or list: Полученные данные из SWAPI.
        """
        url_res = f'{self.api_url}{resource}'
        if '/' in resource:
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
        """
        Получение количества записей по указанному ресурсу из SWAPI.

        Параметры:
            resource (str): Ресурс для запроса количества записей.

        Возвращает:
            int: Количество записей.
        """
        url_res = f'{self.api_url}{resource}'
        json_data = await self.fetch_data(self.session, url_res)
        return json_data.get('count')


class SWIMG(DataSource):
    """
    Класс для работы с изображениями через SWIMG.

    """

    def __init__(self, api_url, logger, session):
        """
        Инициализация объекта класса SWIMG.

        Параметры:
            api_url (str): URL SWIMG.
            logger (logging.Logger): Логгер для записи сообщений.
            session (aiohttp.ClientSession): Сессия для работы с API.
        """
        self.api_url = api_url
        self.logger = logger
        self.session = session
        self.semaphore = asyncio.Semaphore(50)

    def is_valid_image(self, data):
        """
        Проверка на валидность изображения.

        Параметры:
            data: Данные изображения.

        Возвращает:
            bool: Результат проверки 
            (True - валидное изображение, False - невалидное).
        """
        try:
            img = Image.open(BytesIO(data))
            img.verify()
            return True
        except Exception as e:
            self.logger.error(f"Error validating image: {e}")
            return False

    async def fetch(self, session, url):
        """
        Получение данных изображения по указанному URL.

        Параметры:
            session (aiohttp.ClientSession): Сессия для работы с API.
            url (str): URL для получения изображения.

        Возвращает:
            bytes: Данные изображения.
        """
        async with self.semaphore:
            async with session.get(url) as response:
                return await response.read()

    async def get_data(self, ids):
        """
        Получение данных изображений по указанным идентификаторам.

        Параметры:
            ids (list or int): Список идентификаторов или одиночный 
            идентификатор изображения.

        Возвращает:
            list or bytes: Список данных изображений или данные изображения.
        """
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
    """
    Класс для работы с конфигурационными данными в формате JSON.
    """

    def __init__(self, logger, filename='config.json'):
        """
        Инициализация объекта класса JsonConfiguration.

        Параметры:
            logger (logging.Logger): Логгер для записи сообщений.
            filename (str): Имя файла с конфигурационными 
            данными (по умолчанию 'config.json').
        """
        self.filename = filename
        self.data = {}
        self.logger = logger

    def read_config(self):
        """
        Чтение конфигурационных данных из файла JSON.

        Возвращает:
            bool: Результат чтения конфигурационных данных 
            (True - успешно, False - ошибка).
        """
        try:
            with open(self.filename, 'r') as file:
                self.data = json.load(file)
            self.logger.info(
                "Configuration: Successfully read configuration from %s",
                self.filename)
            return True
        except (FileNotFoundError, json.JSONDecodeError):
            self.logger.error(
                "Configuration: Failed to read configuration from %s",
                self.filename)
            return False

    def get_system_config(self, system_name):
        """
        Получение конфигурационных данных для указанной системы.

        Параметры:
            system_name (str): Название системы.

        Возвращает:
            dict: Конфигурационные данные для указанной системы.
        """
        return self.data.get(system_name, {})


class Odoo:
    """
    Класс для взаимодействия с системой Odoo через JSON-RPC API.

    Атрибуты:
        url (str): URL-адрес JSON-RPC API Odoo.
        db (str): Имя базы данных Odoo.
        username (str): Имя пользователя для аутентификации в Odoo.
        password (str): Пароль пользователя для аутентификации в Odoo.
        logger (logging.Logger): Логгер для записи сообщений.
        session (aiohttp.ClientSession): 
            Сеанс aiohttp для выполнения HTTP-запросов.

    Методы:
        conn: Аутентификация в системе Odoo.
        json_rpc: Выполнение JSON-RPC запроса к системе Odoo.
        call: Выполнение JSON-RPC вызова к указанному сервису и методу.
        create: Создание записи в указанной модели в системе Odoo.
    """

    def __init__(self, url, db, username, password, logger, session):
        """
        Инициализация объекта класса Odoo.

        Параметры:
            url (str): URL-адрес JSON-RPC API Odoo.
            db (str): Имя базы данных Odoo.
            username (str): Имя пользователя для аутентификации в Odoo.
            password (str): Пароль пользователя для аутентификации в Odoo.
            logger (logging.Logger): Логгер для записи сообщений.
            session (aiohttp.ClientSession): 
                Сеанс aiohttp для выполнения HTTP-запросов.
        """
        self.url = url + "/jsonrpc"
        self.db = db
        self.username = username
        self.password = password
        self.logger = logger
        self.semaphore = asyncio.Semaphore(20)
        self.session = session

    async def conn(self):
        """
        Аутентификация в системе Odoo.

        Возвращает:
            int: Идентификатор пользователя после успешной аутентификации.
        """
        try:
            self.uid = await self.call(self.url, "common", "login",
                                       self.db, self.username, self.password)
            if self.uid:
                self.logger.info("Odoo: Successfully authenticated")
                return self.uid
            else:
                raise ValueError("Invalid credentials")
        except Exception as e:
            self.logger.error("Odoo: Authentication failed - %s", str(e))
            raise

    async def find_by_name(self, name, resource):
        """
        Поиск ресурсов по имени в системе Odoo.

        Параметры:
            name (str): Имя для поиска.

        Возвращает:
            int or None: Идентификатор найденного ресурса или None, 
            если ресурс не найден.
        """
        try:
            result = await self.call(self.url, "object", "execute_kw",
                                     self.db, self.uid, self.password,
                                     resource, 'search',
                                     [[('name', '=', name)]])
            if result:
                return result[0]
            else:
                return None
        except Exception as e:
            self.logger.error(
                "Odoo: Search resource by name failed - %s", str(e))
            raise

    async def json_rpc(self, url, method, params):
        """
        Выполнение JSON-RPC запроса к системе Odoo.

        Параметры:
            url (str): URL-адрес для выполнения запроса.
            method (str): Метод JSON-RPC.
            params (dict): Параметры запроса.

        Возвращает:
            dict: Ответ от сервера Odoo в формате JSON.
        """
        data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": random.randint(0, 1000000000),
        }
        async with self.semaphore:
            headers = {"Content-Type": "application/json"}
            async with self.session.post(url, json=data,
                                         headers=headers) as response:
                reply = await response.json()
                if reply.get("error"):
                    raise Exception(reply["error"])
                return reply["result"]

    async def call(self, url, service, method, *args):
        """
        Выполнение JSON-RPC вызова к указанному сервису и методу.

        Параметры:
            url (str): URL-адрес для выполнения запроса.
            service (str): Имя сервиса в системе Odoo.
            method (str): Метод JSON-RPC.
            *args: Аргументы метода.

        Возвращает:
            dict: Ответ от сервера Odoo в формате JSON.
        """
        params = {
            "service": service,
            "method": method,
            "args": args
        }
        return await self.json_rpc(url, "call", params)

    async def create(self, uid, model, params):
        """
        Создание записи в указанной модели в системе Odoo.

        Параметры:
            uid (int): Идентификатор пользователя в системе Odoo.
            model (str): Имя модели, в которую необходимо добавить запись.
            params (dict): Параметры для создания записи.

        Возвращает:
            int: Идентификатор созданной записи.
        """
        try:
            [result] = await self.call(self.url, "object", "execute",
                                       self.db, uid, self.password, model,
                                       'create', params)
            self.logger.info(f"Odoo: Successfully create on model {model}")
            return result
        except Exception as e:
            self.logger.error("Odoo: Create failed - %s", str(e))
            raise


class DataProcessor:
    """
    Класс для обработки данных из источников SWAPI и SWIMG 
    и загрузки их в систему Odoo.

    Атрибуты:
        swapi (SWAPI): Объект для взаимодействия с API SWAPI.
        swimg (SWIMG): Объект для взаимодействия с API SWIMG.
        odoo (Odoo): Объект для взаимодействия с системой Odoo.
        logger (logging.Logger): Логгер для записи сообщений.
        loop (asyncio.AbstractEventLoop): Асинхронный цикл событий.

    Методы:
        planet: Загрузка данных о планетах из SWAPI 
            и сохранение в системе Odoo.
        people: Загрузка данных о персонажах из SWAPI 
            и сохранение в системе Odoo.
        process_data: Обработка данных о планетах и персонажах 
        из SWAPI и SWIMG и загрузка их в систему Odoo.
    """

    def __init__(self, swapi, swimg, odoo, logger, loop):
        """
        Инициализация объекта класса DataProcessor.

        Параметры:
            swapi (SWAPI): Объект для взаимодействия с API SWAPI.
            swimg (SWIMG): Объект для взаимодействия с API SWIMG.
            odoo (Odoo): Объект для взаимодействия с системой Odoo.
            logger (logging.Logger): Логгер для записи сообщений.
            loop (asyncio.AbstractEventLoop): Асинхронный цикл событий.
        """
        self.swapi = swapi
        self.swimg = swimg
        self.odoo = odoo
        self.logger = logger
        self.loop = loop

    async def planet(self, id):
        """
        Загрузка данных о планете с заданным идентификатором 
        из SWAPI и сохранение в системе Odoo.

        Параметры:
            id (int): Идентификатор планеты в SWAPI.

        Возвращает:
            tuple: Кортеж с идентификатором планеты и 
            идентификатором созданной записи в системе Odoo.
        """
        swapi_planet = await self.swapi.get_data(f'planets/{id}')
        if not swapi_planet:
            return None
        resource = "res.planet"
        name = swapi_planet.get('name')
        existing_planet_id = await self.odoo.find_by_name(name, resource)
        if existing_planet_id:
            self.logger.info(
                f"Odoo: Planet {name} already exists with" +
                f"ID {existing_planet_id} SWAPI ID {id}")
            return existing_planet_id
        else:
            diameter = swapi_planet.get('diameter')
            population = swapi_planet.get('population')
            rotation_period = swapi_planet.get('rotation_period')
            orbital_period = swapi_planet.get('orbital_period')
            params = [{
                'name': name,
                'diameter': diameter if diameter != 'unknown' else None,
                'population': population if population != 'unknown' else None,
                'rotation_period':
                    rotation_period if rotation_period != 'unknown' else None,
                'orbital_period':
                    orbital_period if orbital_period != 'unknown' else None,
            }]
            planet_id = await self.odoo.create(self.uid, resource, params)
            self.logger.info(
                f"Odoo: Created planet {name} with" +
                f"ID {planet_id} SWAPI ID {id}")
            return planet_id

    async def people(self, id):
        """
        Загрузка данных о персонаже с заданным идентификатором 
        из SWAPI и сохранение в системе Odoo.

        Параметры:
            id (int): Идентификатор персонажа в SWAPI.
        """
        swapi_people = await self.swapi.get_data(f'people/{id}')
        if not swapi_people:
            return
        resource = "res.partner"
        name = swapi_people.get('name')
        existing_people_id = await self.odoo.find_by_name(name, resource)
        if existing_people_id:
            self.logger.info(
                f"Odoo: Person {name} already exists with" +
                f"ID {existing_people_id} SWAPI ID {id}")
            return existing_people_id
        else:
            id_planet_swapi = swapi_people.get(
                'homeworld').rstrip('/').split('/')[-1]

            id_planet_odoo = await self.planet(id_planet_swapi)

            img_data = await self.swimg.get_data(id)
            params = [{
                'name': name,
                'image_1920': base64.b64encode(img_data).decode("utf-8"),
                'planet': id_planet_odoo,
            }]
            person_id = await self.odoo.create(self.uid, resource, params)
            self.logger.info(
                f"Odoo: Created person {name} with" +
                f"ID {person_id} SWAPI ID {id}")
            return person_id

    async def process_data(self):
        """
        Обработка данных о планетах и персонажах из SWAPI 
        и SWIMG и загрузка их в систему Odoo.
        """
        with logging_redirect_tqdm():
            self.uid = await self.odoo.conn()

            swapi_resource = 'people'
            people_count = await self.swapi.get_count(swapi_resource)
            tasks = [self.people(id) for id in range(1, people_count+1)]

            with tqdm(total=people_count, desc="Processing people") as pbar:
                for coro in asyncio.as_completed(tasks):
                    await coro
                    pbar.update(1)


if __name__ == '__main__':
    asyncio.run(main())
