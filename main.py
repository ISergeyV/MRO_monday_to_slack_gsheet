# -*- coding: utf-8 -*-

import time  # Provides various time-related functions, including sleep.
import logging
import concurrent.futures
from googleapiclient.discovery import build

# Import local modules
import config
from src.utils import common as utils
from src.services import monday_service
from src.services import google_service
from src.services import slack_service

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


def main():
    """
    The main function that coordinates the entire data migration process.
    """
    # Load the item number to start from.
    START_ITEM = utils.load_state()

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

    # Load existing IDs once before starting.
    logging.info("Loading list of existing IDs from the sheet...")
    existing_ids = google_service.get_existing_ids(sheets_service)

    try:
        while True:
            # Update START_ITEM before each loop run (in case of a restart).
            START_ITEM = utils.load_state()
            logging.info(
                f"Starting (or resuming) processing from item #{START_ITEM}.")

            try:
                # 4. Process each item
                # Use a generator to get items one by one.
                # Pass START_ITEM to generator to handle skipping efficiently
                for current_item_num, item in enumerate(monday_service.fetch_monday_items_generator(start_item=START_ITEM), start=START_ITEM):

                    item_name = item.get('name')
                    assets = item.get('assets', [])

                    if not item_name:
                        logging.info(
                            f"Item #{current_item_num} with id='{item.get('id')}' has no name. Skipping."
                        )
                        utils.save_state(current_item_num + 1)
                        continue

                    logging.info(
                        f"--- Processing item #{current_item_num}: '{item_name}' ---")

                    if not assets:
                        logging.info(
                            f"Item '{item_name}' has no files (assets).")
                        utils.save_state(current_item_num + 1)
                        continue

                    drive_links = []

                    # 5. Process the item's files in parallel.
                    # max_workers=5 means up to 5 files will be processed simultaneously.
                    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                        # Submit tasks.
                        future_to_asset = {executor.submit(
                            process_asset, asset, item_name): asset for asset in assets}

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
                        # slack_service.send_slack_message(item_name, drive_links)
                        # Add record to Google Sheets
                        google_service.append_to_sheet(sheets_service, item_name, item.get('id'),
                                                       drive_links, existing_ids)
                    else:
                        logging.warning(
                            f"Failed to upload any files for item '{item_name}'.")

                    # Save the number of the next item.
                    utils.save_state(current_item_num + 1)

                # If the loop completed normally (no more data).
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
