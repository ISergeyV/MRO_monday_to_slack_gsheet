# -*- coding: utf-8 -*-

import time  # Provides various time-related functions, including sleep.
import os
import logging
import concurrent.futures
import argparse
from googleapiclient.discovery import build

# Import local modules
import config
from src.utils import common as utils
from src.services import monday_service
from src.services import google_service
from src.services import slack_service
from src.services import playwright_service

# Configure the logging system to output messages to the console.
# The format includes the timestamp, the log level (INFO, ERROR, etc.), and the actual message.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def process_asset(asset, item_name):
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
    content = utils.download_file(public_url)
    if not content:
        return None

    # Compress (if necessary)
    content, final_name = utils.compress_image(content, asset_name)

    # Create a Drive client inside the thread for SSL safety.
    try:
        creds = google_service.get_google_credentials()
        if not creds:
            return None

        drive_service = build(
            'drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        logging.error(f"Error creating Drive client in thread: {e}")
        return None

    # Upload
    return google_service.upload_to_drive(drive_service, content, item_name, final_name)


def process_doc_upload(item_name, markdown_content):
    """
    Uploads markdown content as a file to Google Drive.
    Creates its own drive_service instance for thread safety.
    """
    if not markdown_content:
        return None

    # Create a Drive client inside the thread for SSL safety.
    try:
        creds = google_service.get_google_credentials()
        if not creds:
            return None

        drive_service = build(
            'drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        logging.error(f"Error creating Drive client for doc upload: {e}")
        return None

    # Convert string to bytes
    content_bytes = markdown_content.encode('utf-8')
    filename = "monday_doc.md"

    return google_service.upload_to_drive(drive_service, content_bytes, item_name, filename)


def main():
    """
    The main function that coordinates the entire data migration process.
    """
    parser = argparse.ArgumentParser(description="Monday to Drive Migration")
    parser.add_argument('--mode', choices=['files', 'docs', 'all'], default='all',
                        help="Select what to migrate: 'files' (assets), 'docs' (monday_doc3), or 'all'.")
    parser.add_argument('--debug', action='store_true',
                        help="Enable debug logging.")
    parser.add_argument('--url', action='store_true',
                        help="Only collect Monday Doc URLs without downloading content.")
    parser.add_argument('--auth', action='store_true',
                        help="Run browser authentication to save session state.")
    parser.add_argument('--browser-export', action='store_true',
                        help="Use Playwright to export Markdown from collected URLs in Google Sheet.")
    args = parser.parse_args()

    # Set logging level based on the --debug flag
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load the item number to start from.
    START_ITEM = utils.load_state()

    # --- Special Mode: Browser Authentication ---
    if args.auth:
        logging.info("Starting Browser Authentication Mode...")
        with playwright_service.PlaywrightService(headless=False) as ps:
            ps.authenticate()
        return

    logging.info("--- Starting Migration Script ---")

    # 1. Get Google services
    creds = google_service.get_google_credentials()
    if not creds:
        logging.error(
            "Failed to access Google services. Exiting.")
        return
    logging.info("Google credentials loaded successfully.")

    # Create the Sheets service once (used only in the main thread).
    sheets_service = build('sheets', 'v4', credentials=creds)

    # --- Special Mode: Browser Export from Sheet ---
    if args.browser_export:
        logging.info(
            "Starting Browser Export Mode (using URLs from Google Sheet)...")

        # Create Drive service
        drive_service = build('drive', 'v3', credentials=creds)

        rows = google_service.get_all_rows(sheets_service)
        logging.info(f"Found {len(rows)} rows in Google Sheet.")

        # Initialize Playwright Service
        # Using headless=False to allow visual debugging as requested
        with playwright_service.PlaywrightService(headless=False) as ps:
            for i, row in enumerate(rows):
                # Skip header row if present
                if i == 0 and row and row[0].lower() == 'name':
                    continue

                # Row structure: [Name, ID, Date, DriveLinks, DocURL]
                # Check if row has enough columns
                if len(row) < 5:
                    continue

                item_name = row[0]
                item_id = row[1]
                doc_url = row[4]

                if not doc_url or "monday.com/docs" not in doc_url:
                    continue

                logging.info(f"Processing Item #{item_id}: {item_name}")

                # Download MD
                file_path = ps.download_markdown(doc_url)

                if file_path:
                    # Read file content
                    with open(file_path, 'rb') as f:
                        content = f.read()

                    # Upload to Drive
                    original_filename = os.path.basename(file_path)
                    link = google_service.upload_to_drive(
                        drive_service, content, item_name, original_filename)

                    if link:
                        logging.info(f"Uploaded exported doc to Drive: {link}")
                        # Update sheet with new link in Column F
                        # Row index in Sheets is 1-based, so i + 1
                        google_service.update_cell_link(
                            sheets_service, i + 1, link)

                    # Cleanup
                    os.remove(file_path)
        return

    # Load existing IDs once before starting.
    logging.info("Loading list of existing IDs from the sheet...")
    existing_ids = google_service.get_existing_ids(sheets_service)

    # Буфер для пакетной отправки
    batch_buffer = []
    BATCH_SIZE = 50

    try:
        while True:
            # Update START_ITEM before each loop run (in case of a restart).
            START_ITEM = utils.load_state()
            logging.info(
                f"Starting (or resuming) processing from item #{START_ITEM}.")

            try:
                # 4. Process each item
                # Use a generator to get items one by one.

                fetch_assets = args.mode in ['files', 'all']
                fetch_docs = args.mode in ['docs', 'all'] or args.url
                fetch_docs_content = not args.url

                # Pass START_ITEM to generator to handle skipping efficiently
                for current_item_num, item in enumerate(monday_service.fetch_monday_items_generator(start_item=START_ITEM, fetch_assets=fetch_assets, fetch_docs=fetch_docs, fetch_docs_content=fetch_docs_content), start=START_ITEM):

                    item_name = item.get('name')
                    # Assets (files)
                    assets = item.get('assets', [])

                    if not item_name:
                        logging.info(
                            f"Item #{current_item_num} with id='{item.get('id')}' has no name. Skipping."
                        )
                        utils.save_state(current_item_num + 1)
                        continue

                    logging.info(
                        f"--- Processing item #{current_item_num}: '{item_name}' ---")

                    drive_links = []
                    monday_doc_url = ""

                    # 5. Process the item's files in parallel.
                    # max_workers=5 means up to 5 files will be processed simultaneously.
                    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                        future_to_asset = {}

                        # --- 1. Submit Assets Tasks ---
                        if fetch_assets and assets:
                            future_to_asset.update(
                                {executor.submit(process_asset, asset, item_name): asset for asset in assets})

                        # --- 2. Submit Doc Task ---
                        if fetch_docs and not args.url:
                            column_values = item.get('column_values', [])
                            if column_values:
                                # Find the column with id 'monday_doc3'
                                doc_column = None
                                for col in column_values:
                                    if col.get('id') == 'monday_doc3':
                                        doc_column = col
                                        break
                                # Check if it has file data (DocValue structure)
                                if doc_column and doc_column.get('file'):
                                    try:
                                        blocks = doc_column['file']['doc']['blocks']
                                        md_content = utils.convert_monday_doc_to_md(
                                            blocks)
                                        if md_content:
                                            logging.info(
                                                f"Found Monday Doc for '{item_name}', submitting for upload.")
                                            future_to_asset[executor.submit(
                                                process_doc_upload, item_name, md_content)] = "monday_doc"
                                        else:
                                            logging.warning(
                                                f"Parsed Monday Doc for '{item_name}' resulted in empty content.")
                                    except (KeyError, TypeError) as e:
                                        logging.warning(
                                            f"Failed to parse doc structure for '{item_name}': {e}")
                                elif doc_column:
                                    # Debug: Column exists but no file found.
                                    # This helps identify if the column is empty or has a different structure.
                                    raw_value = doc_column.get('value')
                                    logging.info(
                                        f"Doc column found for '{item_name}' but no file data. Type: {doc_column.get('type')}, Value: {raw_value}")

                        # Collect results as they complete.
                        for future in concurrent.futures.as_completed(future_to_asset):
                            try:
                                link = future.result()
                                if link:
                                    drive_links.append(link)
                            except Exception as e:
                                logging.error(
                                    f"Error processing file in thread: {e}")

                        # Add original Monday.com document URL if exists
                        if fetch_docs:
                            column_values = item.get('column_values', [])
                            if column_values:
                                doc_column = None
                                for col in column_values:
                                    if col.get('id') == 'monday_doc3':
                                        doc_column = col
                                        break
                                if doc_column and doc_column.get('file') and doc_column['file'].get('url'):
                                    monday_doc_url = doc_column['file']['url']
                                    logging.info(
                                        f"Found original Monday.com document URL: {monday_doc_url}")

                    # 8. Send notifications and record data if files were uploaded.
                    if drive_links or monday_doc_url:
                        logging.info(
                            f"Successfully processed item '{item_name}'. Files uploaded: {len(drive_links)}")

                        # Добавляем в буфер вместо мгновенной отправки
                        batch_buffer.append({
                            'name': item_name,
                            'id': item.get('id'),
                            'links': drive_links,
                            'doc_url': monday_doc_url
                        })
                    else:
                        if args.mode == 'docs':
                            logging.info(
                                f"No documents found for item '{item_name}'.")
                        else:
                            logging.warning(
                                f"Failed to upload any files for item '{item_name}'.")

                    # Если буфер заполнился, отправляем данные и сохраняем состояние
                    if len(batch_buffer) >= BATCH_SIZE:
                        logging.info(
                            f"Flushing batch of {len(batch_buffer)} items to Google Sheet...")
                        google_service.sync_batch(
                            sheets_service, batch_buffer, existing_ids)
                        batch_buffer = []
                        utils.save_state(current_item_num + 1)
                    elif not batch_buffer:
                        # Если буфер пуст (например, элементы пропускаются), сохраняем состояние сразу
                        utils.save_state(current_item_num + 1)

                # If the loop completed normally (no more data).
                # Отправляем остатки из буфера
                if batch_buffer:
                    logging.info(
                        f"Flushing final batch of {len(batch_buffer)} items...")
                    google_service.sync_batch(
                        sheets_service, batch_buffer, existing_ids)
                    batch_buffer = []
                    utils.save_state(current_item_num + 1)
                break

            except monday_service.CursorExpiredException:
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
