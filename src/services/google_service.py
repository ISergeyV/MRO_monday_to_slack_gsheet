import io
import time
import logging
import datetime
import re
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload
import config
from src.utils import common as utils


def get_google_credentials():
    try:
        creds = service_account.Credentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_FILE, scopes=config.SCOPES)
        return creds
    except Exception as e:
        logging.error(f"Error loading Google credentials: {e}")
        return None


def upload_to_drive(service, file_content, item_name, original_filename):
    try:
        sanitized_item_name = utils.sanitize_filename(item_name)
        file_name = f"{sanitized_item_name}_{original_filename}"

        try:
            safe_name = file_name.replace("'", "\\'")
            query = f"name = '{safe_name}' and '{config.DRIVE_FOLDER_ID}' in parents and trashed = false"
            response = service.files().list(
                q=query, fields='files(id, webViewLink)', spaces='drive').execute()
            files = response.get('files', [])
            if files:
                logging.info(
                    f"File '{file_name}' already exists. Skipping upload.")
                return files[0].get('webViewLink')
        except Exception as e:
            logging.warning(
                f"Failed to check for duplicates for '{file_name}': {e}")

        file_metadata = {
            'name': file_name,
            'parents': [config.DRIVE_FOLDER_ID]
        }
        media = MediaIoBaseUpload(io.BytesIO(
            file_content), mimetype='application/octet-stream', resumable=True)

        for attempt in range(3):
            try:
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='webViewLink',
                    supportsAllDrives=True
                ).execute()
                logging.info(
                    f"File '{file_name}' successfully uploaded to Google Drive.")
                return file.get('webViewLink')
            except Exception as e:
                logging.warning(
                    f"Upload attempt {attempt + 1} for '{original_filename}' failed: {e}")
                time.sleep(2 ** attempt)

        logging.error(
            f"Failed to upload file '{original_filename}' after 3 attempts.")
        return None
    except Exception as e:
        logging.error(
            f"Critical error preparing upload for '{original_filename}': {e}")
        return None


def get_existing_ids(service):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=config.SHEET_ID,
            range='B:B'
        ).execute()
        rows = result.get('values', [])
        # Возвращаем словарь: ID элемента -> Номер строки (начиная с 1)
        return {str(row[0]): i + 1 for i, row in enumerate(rows) if row}
    except Exception as e:
        logging.error(f"Error retrieving existing IDs: {e}")
        return {}


def _execute_with_retry(request):
    """Executes a Google API request with exponential backoff for rate limits."""
    for attempt in range(5):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 429:
                sleep_time = (2 ** attempt) + 1
                logging.warning(
                    f"Quota exceeded (429). Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                raise
    raise Exception("API request failed after 5 retries")


def sync_batch(service, batch_data, existing_ids):
    """
    Sends a batch of items to Google Sheets.
    batch_data: list of dicts {'name':, 'id':, 'links':, 'doc_url':}
    """
    try:
        new_rows = []
        updates_data = []
        today = datetime.date.today().strftime("%Y-%m-%d")

        for item in batch_data:
            item_id_str = str(item['id'])
            row_values = [
                item['name'],
                item['id'],
                today,
                ", ".join(item['links']),
                item['doc_url']
            ]

            if item_id_str in existing_ids:
                # Подготовка обновления существующей строки
                row_num = existing_ids[item_id_str]
                range_name = f"A{row_num}:E{row_num}"
                updates_data.append({
                    'range': range_name,
                    'values': [row_values]
                })
            else:
                # Подготовка новой строки
                new_rows.append(row_values)

        # 1. Выполняем обновления (Batch Update)
        if updates_data:
            body = {'valueInputOption': 'RAW', 'data': updates_data}
            _execute_with_retry(service.spreadsheets().values().batchUpdate(
                spreadsheetId=config.SHEET_ID, body=body))
            logging.info(
                f"Updated {len(updates_data)} records in Google Sheet.")

        # 2. Выполняем добавления (Append)
        if new_rows:
            body = {'values': new_rows}
            response = _execute_with_retry(service.spreadsheets().values().append(
                spreadsheetId=config.SHEET_ID, range='A1',
                valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body))

            # Обновляем existing_ids для новых записей
            updated_range = response.get('updates', {}).get('updatedRange')
            if updated_range:
                # Пример updatedRange: 'Sheet1!A100:E105'
                match = re.search(r'[A-Z]+(\d+):', updated_range)
                if match:
                    start_row = int(match.group(1))
                    for i, row in enumerate(new_rows):
                        existing_ids[str(row[1])] = start_row + i

            logging.info(
                f"Appended {len(new_rows)} new records to Google Sheet.")

    except Exception as e:
        logging.error(f"Error syncing batch to Google Sheet: {e}")
