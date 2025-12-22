# -*- coding: utf-8 -*-

# Import standard and third-party libraries required for the script's operation.
# Provides a portable way of using operating system dependent functionality.
import os
import requests  # Standard library for making HTTP requests in Python.
import datetime  # Supplies classes for manipulating dates and times.
import time  # Provides various time-related functions, including sleep.
import re  # Provides regular expression matching operations.
# Defines functions and classes which implement a flexible event logging system.
import logging
# Loads environment variables from a .env file into os.environ.
from dotenv import load_dotenv
# Credentials for Google Service Accounts.
from google.oauth2 import service_account
# Function to build a service object for interacting with Google APIs.
from googleapiclient.discovery import build
# Class to upload media content to Google Drive from an in-memory stream.
from googleapiclient.http import MediaIoBaseUpload
# Implements stream handling, allowing us to work with file data in memory.
import io
# Provides a high-level interface for asynchronously executing callables.
import concurrent.futures
# Python Imaging Library, used here for opening and compressing images.
from PIL import Image
# Client for interacting with the Slack Web API.
from slack_sdk import WebClient
# Exception class for Slack API errors.
from slack_sdk.errors import SlackApiError

# Load environment variables from a .env file.
# This allows storing configuration and secrets separately from the codebase.
load_dotenv()

# Configure the logging system to output messages to the console.
# The format includes the timestamp, the log level (INFO, ERROR, etc.), and the actual message.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Monday.com Configuration ---
# Retrieve the Monday.com API key, API endpoint URL, and the specific Board ID from environment variables.
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_BOARD_ID = os.getenv("MONDAY_BOARD_ID")

# --- Google Drive Configuration ---
# Retrieve the path to the service account JSON key file, the target folder ID for uploads,
# and define the necessary OAuth2 scopes for Drive and Sheets access.
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']

# --- Google Sheets Configuration ---
# Retrieve the ID of the Google Sheet used for logging migration results.
SHEET_ID = os.getenv("SHEET_ID")

# --- Slack Configuration ---
# Retrieve the Slack Bot Token and the Channel ID where notifications will be posted.
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

# Define the filename for storing the migration state (the index of the last processed item).
# This allows the script to resume from where it left off in case of interruption.
STATE_FILE = "migration_state.txt"


class CursorExpiredException(Exception):
    """
    Custom exception raised when the Monday.com pagination cursor expires.
    This signals the main loop to refresh the cursor.
    """
    pass


def get_google_credentials():
    """
    Loads Google API credentials from the service account JSON file.
    Returns a Credentials object or None if loading fails.
    """
    try:
        # Load credentials from the file specified in the environment variables.
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
        logging.info("Google credentials loaded successfully.")
        return creds
    except Exception as e:
        # In case of an authentication error, log the exception and return None.
        logging.error(f"Error loading Google credentials: {e}")
        return None


def fetch_monday_page(cursor=None):
    """
    Performs a GraphQL query to the Monday.com API to fetch a single page
    of items from the specified board using cursor-based pagination.

    Args:
        cursor (str, optional): The cursor string for the next page. If None, fetches the first page.

    Returns:
        tuple: A tuple containing a list of items and the next cursor string (or None).
    """
    variables = {}
    if cursor:
        query = """
        query ($cursor: String!) {
          next_items_page (cursor: $cursor, limit: 25) {
            cursor
            items {
              id
              name
              assets {
                id
                name
                public_url
                file_extension
              }
            }
          }
        }
        """
        variables['cursor'] = cursor
    else:
        query = f"""
        query {{
          boards (ids: {MONDAY_BOARD_ID}) {{
            items_page (limit: 25) {{
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

    # Retry attempts for the request in case of temporary network errors (e.g., timeouts).
    for attempt in range(3):
        try:
            response = requests.post(MONDAY_API_URL, json={
                                     'query': query, 'variables': variables}, headers=headers)
            response.raise_for_status()
            json_response = response.json()

            # Check for specific GraphQL errors in the response body.
            if 'errors' in json_response:
                for error in json_response['errors']:
                    if 'CursorExpiredError' in error.get('message', ''):
                        logging.warning("CursorExpiredError received.")
                        raise CursorExpiredException("Cursor expired")

                logging.error(f"GraphQL Errors: {json_response['errors']}")
                return [], None

            data = json_response.get('data', {})
            if cursor:
                items_page = data.get('next_items_page', {})
            else:
                boards = data.get('boards', [])
                if not boards:
                    logging.warning(
                        "Response from Monday.com does not contain board data.")
                    return [], None
                items_page = boards[0].get('items_page', {})

            items = items_page.get('items', [])
            next_cursor = items_page.get('cursor')
            return items, next_cursor

        except requests.exceptions.RequestException as e:
            logging.warning(
                f"Attempt {attempt + 1} failed: {e}. Retrying in {2 ** attempt} seconds...")
            time.sleep(2 ** attempt)  # Exponential backoff

    logging.error("Failed to fetch data from Monday.com after 3 attempts.")
    return [], None


def fetch_monday_items_generator():
    """
    A generator that fetches items from the Monday.com board page by page and yields them one by one.
    This allows for stream processing of data without loading everything into memory
    and ensures that public_urls for assets are fresh for each new page.
    """
    cursor = None
    while True:
        items, cursor = fetch_monday_page(cursor)
        if items:
            logging.info(f"Fetched a page containing {len(items)} items.")
            for item in items:
                yield item
        if not cursor:
            break


def download_file(url):
    """
    Downloads a file from the specified URL and returns its binary content.
    Returns None if the download fails.
    """
    try:
        # Perform a GET request to download the file.
        response = requests.get(url)
        # Check for HTTP errors.
        response.raise_for_status()
        # Return the file content.
        return response.content
    except requests.exceptions.RequestException as e:
        # Log an error if the download fails.
        logging.error(f"Failed to download file from {url}: {e}")
        return None


def compress_image(file_content, filename, target_size_mb=1.0):
    """
    Compresses an image (JPEG/PNG) to the specified target size (default 1.0 MB).
    Returns a tuple (file_content, filename).
    If compression is not possible or not needed, returns the original data.
    """
    try:
        # If the file is already smaller than the target size, return it as is.
        if len(file_content) <= target_size_mb * 1024 * 1024:
            return file_content, filename

        img = Image.open(io.BytesIO(file_content))

        # Process only JPEG and PNG files.
        if img.format not in ['JPEG', 'PNG']:
            return file_content, filename

        # Convert to RGB (necessary for saving PNG as JPEG).
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')

        output_io = io.BytesIO()
        quality = 85

        # Save as JPEG with optimization.
        img.save(output_io, format='JPEG', quality=quality, optimize=True)

        # If the file is still larger than the target size, reduce quality in a loop.
        while output_io.tell() > target_size_mb * 1024 * 1024 and quality > 20:
            output_io.seek(0)
            output_io.truncate()
            quality -= 10
            img.save(output_io, format='JPEG', quality=quality, optimize=True)

        # Change the extension to .jpg since we converted the file.
        base_name = os.path.splitext(filename)[0]
        new_filename = f"{base_name}.jpg"

        logging.info(
            f"Compressed: {filename} ({len(file_content)/1024/1024:.2f}MB) -> {new_filename} ({output_io.tell()/1024/1024:.2f}MB)")
        return output_io.getvalue(), new_filename

    except Exception as e:
        logging.warning(
            f"Failed to compress {filename}: {e}. Using original.")
        return file_content, filename


def sanitize_filename(name):
    """
    Sanitizes a string to ensure it is a valid and safe filename by removing illegal characters.
    """
    # Remove characters that are invalid in most file systems.
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    # Replace multiple spaces with a single one and trim whitespace.
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def upload_to_drive(service, file_content, item_name, original_filename):
    """
    Uploads a file to Google Drive into the specified folder and returns its public link.
    Handles duplicate checks and retries on failure.
    """
    try:
        # Sanitize the name and form the full filename.
        sanitized_item_name = sanitize_filename(item_name)
        file_name = f"{sanitized_item_name}_{original_filename}"

        # Check if a file with the same name already exists in the target folder.
        try:
            # Escape single quotes for a safe query.
            safe_name = file_name.replace("'", "\\'")
            query = f"name = '{safe_name}' and '{DRIVE_FOLDER_ID}' in parents and trashed = false"
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

        # File metadata: name and parent folder.
        file_metadata = {
            'name': file_name,
            'parents': [DRIVE_FOLDER_ID]
        }
        # Create a media upload object for the content.
        media = MediaIoBaseUpload(io.BytesIO(
            file_content), mimetype='application/octet-stream', resumable=True)

        # Retry upload attempts.
        for attempt in range(3):
            try:
                # Execute the request to create the file in Google Drive.
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
                time.sleep(2 ** attempt)  # Exponential backoff

        logging.error(
            f"Failed to upload file '{original_filename}' after 3 attempts.")
        return None
    except Exception as e:
        logging.error(
            f"Critical error preparing upload for '{original_filename}': {e}")
        return None


def send_slack_message(item_name, drive_links):
    """
    Sends a formatted message to Slack with the item name and links to the files on Google Drive.
    """
    # Check if Slack token and channel are configured.
    if not SLACK_TOKEN or not SLACK_CHANNEL:
        logging.warning(
            "Slack token or channel not configured. Skipping Slack notification.")
        return

    # Initialize the Slack client.
    client = WebClient(token=SLACK_TOKEN)
    # Format the message using Slack Block Kit for better presentation.
    message = {
        "channel": SLACK_CHANNEL,
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*New Item: {item_name}*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Attachments:\n" + "\n".join([f"<{link}|{link.split('/')[-2]}>" for link in drive_links])
                }
            }
        ]
    }
    try:
        # Send the message.
        client.chat_postMessage(**message)
        logging.info(
            f"Sent Slack notification for item: {item_name}")
    except SlackApiError as e:
        # Log an error if sending fails.
        logging.error(
            f"Error sending Slack message: {e.response['error']}")


def get_existing_ids(service):
    """
    Retrieves a set of all item IDs that are already recorded in the Google Sheet.
    This is used to prevent duplicate entries.
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range='B:B'
        ).execute()
        rows = result.get('values', [])
        return {str(row[0]) for row in rows if row}
    except Exception as e:
        logging.error(f"Error retrieving existing IDs: {e}")
        return set()


def append_to_sheet(service, item_name, item_id, drive_links, existing_ids):
    """
    Appends a new row to the Google Sheet with the item name, ID, date, and file links.
    Checks against the local cache of existing IDs to avoid duplicates.
    """
    try:
        if str(item_id) in existing_ids:
            logging.info(
                f"Record with ID {item_id} already exists in the sheet. Skipping.")
            return

        # Get the current date in YYYY-MM-DD format.
        today = datetime.date.today().strftime("%Y-%m-%d")
        # Format the data for the new row.
        values = [[item_name, item_id, today, ", ".join(drive_links)]]
        body = {'values': values}
        # Execute the request to append the row to the sheet.
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            # The range to find the last row (you can specify a sheet name, e.g., 'Sheet1!A1').
            range='A1',
            valueInputOption='RAW',  # Data is inserted as is.
            insertDataOption='INSERT_ROWS',  # Insert as new rows.
            body=body
        ).execute()
        logging.info(
            f"Data for item '{item_name}' appended to Google Sheet.")

        # Add the ID to the local cache.
        existing_ids.add(str(item_id))
    except Exception as e:
        # Log an error if writing fails.
        logging.error(f"Error appending data to Google Sheet: {e}")


def load_state():
    """
    Loads the item number to start from, from the state file.
    Returns 1 if the file does not exist or cannot be read.
    """
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                val = f.read().strip()
                if val.isdigit():
                    return int(val)
        except Exception as e:
            logging.warning(f"Failed to read state file: {e}")
    return 1


def save_state(item_num):
    """
    Saves the number of the next item to be processed to the state file.
    This ensures the script can resume from this point.
    """
    try:
        with open(STATE_FILE, 'w') as f:
            f.write(str(item_num))
    except Exception as e:
        logging.error(f"Failed to save state: {e}")


def process_asset(asset, item_name, creds):
    """
    Processes a single file: downloads, compresses, and uploads it to Drive.
    Creates its own drive_service instance for thread safety.
    """
    public_url = asset.get('public_url')
    asset_name = asset.get('name')

    if not public_url or not asset_name:
        return None

    # Download
    logging.info(f"Downloading file: {asset_name}")
    content = download_file(public_url)
    if not content:
        return None

    # Compress (if necessary)
    content, final_name = compress_image(content, asset_name)

    # Create a Drive client inside the thread for SSL safety.
    try:
        drive_service = build(
            'drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        logging.error(f"Error creating Drive client in thread: {e}")
        return None

    # Upload
    return upload_to_drive(drive_service, content, item_name, final_name)


def main():
    """
    The main function that coordinates the entire data migration process.
    """
    # Load the item number to start from.
    START_ITEM = load_state()

    logging.info("--- Starting Migration Script ---")

    # 1. Get Google services
    creds = get_google_credentials()
    if not creds:
        logging.error(
            "Failed to access Google services. Exiting.")
        return

    # Create the Sheets service once (used only in the main thread).
    sheets_service = build('sheets', 'v4', credentials=creds)

    # Load existing IDs once before starting.
    logging.info("Loading list of existing IDs from the sheet...")
    existing_ids = get_existing_ids(sheets_service)

    try:
        while True:
            # Update START_ITEM before each loop run (in case of a restart).
            START_ITEM = load_state()
            logging.info(
                f"Starting (or resuming) processing from item #{START_ITEM}.")

            try:
                # 4. Process each item
                # Use a generator to get items one by one.
                for current_item_num, item in enumerate(fetch_monday_items_generator(), start=1):

                    # Skip items that have already been processed.
                    if current_item_num < START_ITEM:
                        if current_item_num % 200 == 0:
                            logging.info(
                                f"Skipping processed items... (current #{current_item_num})")
                        continue

                    item_name = item.get('name')
                    assets = item.get('assets', [])

                    if not item_name:
                        logging.info(
                            f"Item #{current_item_num} with id='{item.get('id')}' has no name. Skipping."
                        )
                        save_state(current_item_num + 1)
                        continue

                    logging.info(
                        f"--- Processing item #{current_item_num}: '{item_name}' ---")

                    if not assets:
                        logging.info(
                            f"Item '{item_name}' has no files (assets).")
                        save_state(current_item_num + 1)
                        continue

                    drive_links = []

                    # 5. Process the item's files in parallel.
                    # max_workers=5 means up to 5 files will be processed simultaneously.
                    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                        # Submit tasks.
                        future_to_asset = {executor.submit(
                            process_asset, asset, item_name, creds): asset for asset in assets}

                        # Collect results as they complete.
                        for future in concurrent.futures.as_completed(future_to_asset):
                            try:
                                link = future.result()
                                if link:
                                    drive_links.append(link)
                            except Exception as e:
                                logging.error(
                                    f"Error processing file in thread: {e}")

                    # 8. Send notifications and record data if files were uploaded.
                    if drive_links:
                        logging.info(
                            f"Successfully uploaded {len(drive_links)} files for item '{item_name}'.")
                        # Send Slack message
                        # send_slack_message(item_name, drive_links)
                        # Add record to Google Sheets
                        append_to_sheet(sheets_service, item_name, item.get('id'),
                                        drive_links, existing_ids)
                    else:
                        logging.warning(
                            f"Failed to upload any files for item '{item_name}'.")

                    # Save the number of the next item.
                    save_state(current_item_num + 1)

                # If the loop completed normally (no more data).
                break

            except CursorExpiredException:
                logging.warning(
                    "Monday.com cursor expired. Restarting process to obtain a new cursor...")
                time.sleep(5)
                continue
    except KeyboardInterrupt:
        logging.info("User interruption (Ctrl+C).")
    finally:
        logging.info("--- Script finished ---")


# Script entry point
if __name__ == "__main__":
    main()
