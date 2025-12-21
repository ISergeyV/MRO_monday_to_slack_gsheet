# -*- coding: utf-8 -*-

# Импорт необходимых библиотек
import os  # Для работы с переменными окружения
import requests  # Для выполнения HTTP-запросов
import datetime  # Для работы с датами
import logging  # Для логирования
from dotenv import load_dotenv  # Для загрузки переменных окружения из .env файла
from google.oauth2 import service_account  # Для аутентификации в Google API
from googleapiclient.discovery import build  # Для создания клиентов Google API
from googleapiclient.http import MediaIoBaseUpload  # Для загрузки медиафайлов в Google Drive
import io  # Для работы с потоками ввода-вывода в памяти
from slack_sdk import WebClient  # Для взаимодействия с Slack API
from slack_sdk.errors import SlackApiError  # Для обработки ошибок Slack API

# Загрузка переменных окружения из файла .env
# Это позволяет хранить конфигурацию и секреты отдельно от кода
load_dotenv()

# Настройка логирования для отслеживания работы скрипта
# Логи будут выводиться в консоль с указанием времени, уровня и сообщения
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация Monday.com ---
# Получение API ключа, URL и ID доски из переменных окружения
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_BOARD_ID = os.getenv("MONDAY_BOARD_ID")

# --- Конфигурация Google Drive ---
# Путь к файлу с учетными данными Google, ID папки для загрузки и области доступа API
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

# --- Конфигурация Google Sheets ---
# ID таблицы для записи данных
SHEET_ID = os.getenv("SHEET_ID")

# --- Конфигурация Slack ---
# Токен и ID канала для отправки сообщений
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

def get_google_services():
    """
    Аутентифицируется в Google API с использованием сервисного аккаунта
    и возвращает объекты для работы с Google Drive и Google Sheets.
    """
    try:
        # Загрузка учетных данных из файла, указанного в .env
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
        # Создание клиента для Google Drive API
        drive_service = build('drive', 'v3', credentials=creds)
        # Создание клиента для Google Sheets API
        sheets_service = build('sheets', 'v4', credentials=creds)
        logging.info("Успешная аутентификация в Google API.")
        return drive_service, sheets_service
    except Exception as e:
        # В случае ошибки аутентификации, логируем ее и возвращаем None
        logging.error(f"Ошибка аутентификации в Google: {e}")
        return None, None

def fetch_monday_page(cursor=None):
    """
    Выполняет GraphQL-запрос к API Monday.com для получения одной страницы
    элементов (items) с указанной доски (board).
    Поддерживает пагинацию через курсоры.
    """
    if cursor:
        query = f"""
        query {{
          next_items_page (cursor: "{cursor}", limit: 100) {{
            cursor
            items {{
              id
              name
              assets {{
                id
                name
                public_url
                file_extension
              }}
            }}
          }}
        }}
        """
    else:
        query = f"""
        query {{
          boards (ids: {MONDAY_BOARD_ID}) {{
            items_page (limit: 100) {{
              cursor
              items {{
                id
                name
                assets {{
                  id
                  name
                  public_url
                  file_extension
                }}
              }}
            }}
          }}
        }}
        """
    
    headers = {"Authorization": MONDAY_API_KEY}
    try:
        response = requests.post(MONDAY_API_URL, json={'query': query}, headers=headers)
        response.raise_for_status()
        data = response.json().get('data', {})
        
        if cursor:
            items_page = data.get('next_items_page', {})
        else:
            boards = data.get('boards', [])
            if not boards:
                logging.warning("Ответ от Monday.com не содержит данных по доскам.")
                return [], None
            items_page = boards[0].get('items_page', {})

        items = items_page.get('items', [])
        next_cursor = items_page.get('cursor')
        return items, next_cursor

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении данных из Monday.com: {e}")
        return [], None

def fetch_all_monday_data():
    """
    Получает все элементы с доски Monday.com, используя пагинацию.
    """
    all_items = []
    cursor = None
    while True:
        items, cursor = fetch_monday_page(cursor)
        if items:
            all_items.extend(items)
            logging.info(f"Получено {len(items)} элементов. Всего: {len(all_items)}.")
        if not cursor:
            break
    logging.info(f"Завершено получение данных. Всего элементов: {len(all_items)}.")
    return all_items

def download_file(url):
    """Скачивает файл по указанному URL и возвращает его бинарное содержимое."""
    try:
        # Выполнение GET-запроса для скачивания файла
        response = requests.get(url)
        # Проверка на наличие HTTP-ошибок
        response.raise_for_status()
        # Возвращаем содержимое файла
        return response.content
    except requests.exceptions.RequestException as e:
        # Логирование ошибки при сбое скачивания
        logging.error(f"Не удалось скачать файл с {url}: {e}")
        return None

def upload_to_drive(service, file_content, item_name, original_filename):
    """
    Загружает файл в Google Drive в указанную папку и возвращает публичную ссылку на него.
    """
    try:
        # Формирование нового имени файла для уникальности
        file_name = f"{item_name}_{original_filename}"
        # Метаданные файла: имя и родительская папка
        file_metadata = {
            'name': file_name,
            'parents': [DRIVE_FOLDER_ID]
        }
        # Создание объекта для загрузки медиа-контента
        media = MediaIoBaseUpload(io.BytesIO(file_content), mimetype='application/octet-stream', resumable=True)
        # Выполнение запроса на создание файла в Google Drive
        file = service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields='webViewLink',
            supportsAllDrives=True
            ).execute()
        logging.info(f"Файл '{file_name}' успешно загружен в Google Drive.")
        # Возвращаем ссылку для просмотра файла
        return file.get('webViewLink')
    except Exception as e:
        # Логирование ошибки при сбое загрузки
        logging.error(f"Ошибка при загрузке файла '{original_filename}' в Google Drive: {e}")
        return None

def send_slack_message(item_name, drive_links):
    """
    Отправляет отформатированное сообщение в Slack с именем элемента и ссылками на файлы в Google Drive.
    """
    # Проверка наличия токена и канала Slack
    if not SLACK_TOKEN or not SLACK_CHANNEL:
        logging.warning("Токен или канал Slack не настроены. Пропуск уведомления в Slack.")
        return

    # Инициализация клиента Slack
    client = WebClient(token=SLACK_TOKEN)
    # Формирование сообщения с использованием Slack Block Kit для лучшего форматирования
    message = {
        "channel": SLACK_CHANNEL,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Новый элемент: {item_name}*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Вложения:\n" + "\n".join([f"<{link}|{link.split('/')[-2]}>" for link in drive_links])
                }
            }
        ]
    }
    try:
        # Отправка сообщения
        client.chat_postMessage(**message)
        logging.info(f"Отправлено уведомление в Slack для элемента: {item_name}")
    except SlackApiError as e:
        # Логирование ошибки при сбое отправки
        logging.error(f"Ошибка при отправке сообщения в Slack: {e.response['error']}")


def append_to_sheet(service, item_name, drive_links):
    """
    Добавляет новую строку в Google Sheet с именем элемента, датой и ссылками на файлы.
    """
    try:
        # Получение текущей даты в формате YYYY-MM-DD
        today = datetime.date.today().strftime("%Y-%m-%d")
        # Формирование данных для новой строки
        values = [[item_name, today, ", ".join(drive_links)]]
        body = {'values': values}
        # Выполнение запроса на добавление строки в таблицу
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range='A1',  # Диапазон для поиска последней строки (можно указать имя листа, например 'Sheet1!A1')
            valueInputOption='RAW',  # Данные вставляются как есть
            insertDataOption='INSERT_ROWS',  # Вставлять как новые строки
            body=body
        ).execute()
        logging.info(f"Данные по элементу '{item_name}' добавлены в Google Sheet.")
    except Exception as e:
        # Логирование ошибки при сбое записи
        logging.error(f"Ошибка при добавлении данных в Google Sheet: {e}")

def main():
    """
    Главная функция, координирующая весь процесс миграции данных.
    """
    # Устанавливаем номер элемента, с которого нужно начать обработку.
    # 1 соответствует первому элементу.
    START_ITEM = 26
    
    logging.info("--- Запуск скрипта миграции ---")

    # 1. Получение сервисов Google
    drive_service, sheets_service = get_google_services()
    if not drive_service or not sheets_service:
        logging.error("Не удалось получить доступ к сервисам Google. Завершение работы.")
        return

    # 2. Получение всех данных из Monday.com
    all_items = fetch_all_monday_data()
    if not all_items:
        logging.info("Элементов для обработки не найдено.")
        return

    # 3. Определяем, какие элементы обрабатывать
    if START_ITEM > len(all_items):
        logging.info(f"Значение START_ITEM ({START_ITEM}) больше, чем общее количество элементов ({len(all_items)}). Обработка не требуется.")
        return
        
    items_to_process = all_items[START_ITEM-1:]
    logging.info(f"Всего получено {len(all_items)} элементов. Начинаем обработку с элемента №{START_ITEM} ({len(items_to_process)} элементов).")

    # 4. Обработка каждого элемента
    for item in items_to_process:
        item_name = item.get('name')
        assets = item.get('assets', [])

        if not item_name:
            logging.warning(f"Элемент с id='{item.get('id')}' не имеет имени. Пропуск.")
            continue
        
        logging.info(f"--- Обработка элемента: '{item_name}' ---")

        if not assets:
            logging.info(f"Для элемента '{item_name}' нет файлов (assets).")
            continue

        drive_links = []
        # 5. Обработка каждого файла (asset) в элементе
        for asset in assets:
            public_url = asset.get('public_url')
            asset_name = asset.get('name')

            if not public_url or not asset_name:
                logging.warning(f"У файла для элемента '{item_name}' отсутствует URL или имя. Пропуск файла.")
                continue

            # 6. Скачивание файла
            logging.info(f"Скачивание файла: {asset_name}")
            file_content = download_file(public_url)

            # 7. Загрузка файла в Google Drive
            if file_content:
                drive_link = upload_to_drive(drive_service, file_content, item_name, asset_name)
                if drive_link:
                    drive_links.append(drive_link)
        
        # 8. Отправка уведомлений и запись данных, если были загружены файлы
        if drive_links:
            logging.info(f"Для элемента '{item_name}' успешно загружено {len(drive_links)} файлов.")
            # Отправка сообщения в Slack
            #send_slack_message(item_name, drive_links)
            # Добавление записи в Google Sheets
            append_to_sheet(sheets_service, item_name, drive_links)
        else:
            logging.warning(f"Для элемента '{item_name}' не удалось загрузить ни одного файла.")
    
    logging.info("--- Скрипт завершил работу ---")

# Точка входа в скрипт
if __name__ == "__main__":
    main()
