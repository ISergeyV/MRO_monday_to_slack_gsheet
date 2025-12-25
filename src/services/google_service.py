import io
import time
import logging
import datetime
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
        return {str(row[0]) for row in rows if row}
    except Exception as e:
        logging.error(f"Error retrieving existing IDs: {e}")
        return set()


def append_to_sheet(service, item_name, item_id, drive_links, existing_ids):
    try:
        if str(item_id) in existing_ids:
            logging.info(
                f"Record with ID {item_id} already exists in the sheet. Skipping.")
            return

        today = datetime.date.today().strftime("%Y-%m-%d")
        values = [[item_name, item_id, today, ", ".join(drive_links)]]
        body = {'values': values}
        service.spreadsheets().values().append(
            spreadsheetId=config.SHEET_ID,
            range='A1',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        logging.info(
            f"Data for item '{item_name}' appended to Google Sheet.")

        existing_ids.add(str(item_id))
    except Exception as e:
        logging.error(f"Error appending data to Google Sheet: {e}")
