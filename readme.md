# Описание программы

Этот проект представляет собой асинхронный скрипт на Python для обработки данных из API Star Wars (SWAPI) и загрузки их в систему управления контентом Odoo.

## Установка

Для запуска этой программы вам понадобится Python версии 3.7 или выше.

1. Клонируйте репозиторий:

```bash
git clone https://github.com/Ekzitus/odoo.git
```
2. Перейдите в каталог проекта:

```bash
cd your_project
```

3. Создайте и активируйте вирутальное окружение

```bash
python -m venv .venv
source .venv/bin/activate   # Для Windows: .venv\Scripts\activate
```

4. Установите зависимости:

```bash
pip install -r requirements.txt
```

5. Запустите программу:

```bash
python main.py --config config.json
```

## Использование

Программа читает конфигурационный файл config.json, в котором содержатся данные для подключения к системам SWAPI, SWIMG и Odoo. После успешного подключения к системам данные загружаются из SWAPI, а затем изображения с помощью SWIMG. Полученные данные обрабатываются и загружаются в систему Odoo.

## Конфигурационный файл

Программа использует конфигурационный файл `config.json`, который содержит следующие параметры:

```json
{
    "Odoo": {
        "url": "http://127.0.0.1:8069",
        "db": "db",
        "username": "username",
        "password": "pass"
    },
    "SWAPI": {
        "url": "https://swapi.dev/api/"
    },
    "SWIMG": {
        "url": "https://starwars-visualguide.com/assets/img/characters/"
    }
}
```
- **ODOO**: Содержит информацию для подключения к системе Odoo, включая URL, имя базы данных, имя пользователя и пароль.
- **SWAPI**: Содержит URL-адрес API для получения данных из SWAPI (Star Wars API).
- **SWIMG**: Содержит URL-адрес API для получения изображений персонажей Star Wars.


## Авторы

- **Aвтор** - [@ekzitus](https://github.com/Ekzitus)