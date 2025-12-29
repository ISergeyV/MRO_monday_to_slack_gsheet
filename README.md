# Monday.com to Google Drive & Sheets Migration Tool

This robust, production-ready automation script migrates data and file assets from a Monday.com board to Google Drive and Google Sheets. It is engineered for high reliability, performance, and data integrity when handling large datasets.

## Key Features

- **🚀 Resumable Migration:** The script tracks its progress in `migration_state.txt`. If the process is interrupted (e.g., internet failure, manual stop, or crash), it automatically resumes from the last processed item upon restart.
- **⚡ Parallel Processing:** Utilizes `ThreadPoolExecutor` to download, compress, and upload multiple files simultaneously (up to 5 threads), significantly reducing total migration time.
- **🖼️ Smart Image Compression:** Automatically detects large images (JPEG/PNG > 1MB) and compresses them using `Pillow` before uploading, optimizing Google Drive storage usage while maintaining visual quality.
- **🔄 Advanced Pagination & Error Handling:**
  - Efficiently fetches data using GraphQL cursors.
  - **Auto-Recovery:** Automatically handles `CursorExpiredError` from Monday.com by refreshing the cursor and fast-forwarding to the current position.
  - **Retries:** Implements exponential backoff retries for network requests to Monday.com and Google Drive.
- **🛡️ Data Integrity & Duplicate Prevention:**
  - **Google Drive:** Checks if a file with the sanitized name already exists in the target folder to avoid duplicates.
  - **Google Sheets:** Caches existing Item IDs at startup and checks against them to prevent duplicate rows in the report.
  - **Thread Safety:** Creates isolated Google API client instances for each thread to prevent SSL/Socket errors during parallel execution.
  - **Sanitization:** Cleans filenames of illegal characters to ensure compatibility with all file systems.
- **🔔 Integrations:**
  - **Google Sheets:** Logs Item Name, Monday Item ID, Date, and Drive Links.
  - **Slack:** Capable of sending notifications with links to uploaded files (configurable).

## Technical Workflow

1.  **Initialization:** Loads environment variables and authenticates with Google Services.
2.  **State Loading:** Reads `migration_state.txt` to determine the starting Item index.
3.  **Caching:** Fetches all existing Item IDs from the Google Sheet to build a local cache for duplicate checking.
4.  **Data Fetching:** Uses a Python generator to fetch items from Monday.com in pages of 25, ensuring fresh public URLs for assets.
5.  **Processing Loop:**
    - Skips items already processed or existing in the Google Sheet.
    - Downloads assets in parallel.
    - Compresses images if necessary.
    - Uploads to Google Drive (checking for duplicates).
6.  **Reporting:** Appends a summary row to Google Sheets and updates the local state file.
7.  **Completion:** Handles `KeyboardInterrupt` (Ctrl+C) gracefully.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd <repository-folder>
```

### 2. Create a Virtual Environment

It is recommended to use a virtual environment to manage the project dependencies.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

Install the required Python libraries using the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the root directory of the project and add the following environment variables. You can use the `.env.example` file as a template.

```
MONDAY_API_KEY=your_monday_api_key
MONDAY_BOARD_ID=your_monday_board_id
SLACK_TOKEN=your_slack_token
SLACK_CHANNEL=your_slack_channel_id
GOOGLE_CREDENTIALS_FILE=path/to/your/google/credentials.json
DRIVE_FOLDER_ID=your_google_drive_folder_id
SHEET_ID=your_google_sheet_id
```

- `MONDAY_API_KEY`: Your Monday.com API key.
- `MONDAY_BOARD_ID`: The ID of the Monday.com board you want to process.
- `SLACK_TOKEN`: Your Slack bot token.
- `SLACK_CHANNEL`: The ID of the Slack channel where you want to send notifications.
- `GOOGLE_CREDENTIALS_FILE`: The path to your Google service account credentials JSON file.
- `DRIVE_FOLDER_ID`: The ID of the Google Drive folder where you want to upload the files.
- `SHEET_ID`: The ID of the Google Sheet where you want to append the data.

### 5. Google API Credentials

To use the Google Drive and Google Sheets APIs, you need to create a service account and enable the APIs.

1.  **Create a Google Cloud Project:** Go to the [Google Cloud Console](https://console.cloud.google.com/) and create a new project.
2.  **Enable APIs:** In your project, enable the "Google Drive API" and "Google Sheets API".
3.  **Create a Service Account:** Create a service account and download the credentials as a JSON file.
4.  **Share Google Drive Folder:** Share the Google Drive folder with the service account's email address.
5.  **Share Google Sheet:** Share the Google Sheet with the service account's email address.

## Running the Script

Once you have completed the setup, you can run the script using the following command:

```bash
python main.py
```

The script will fetch the last 50 items from Monday.com, process the assets, upload them to Google Drive, and send notifications to Slack and Google Sheets.

По умолчанию (все вместе):

bash
python main.py
или

bash
python main.py --mode all
Только документы (monday_doc3):

bash
python main.py --mode docs
Только файлы (как было раньше):

bash
python main.py --mode files