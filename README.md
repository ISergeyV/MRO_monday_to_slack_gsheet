# Monday.com to Google Sheets and Slack Integration

This script automates the process of fetching data from Monday.com, uploading files to Google Drive, and sending notifications to Slack and Google Sheets.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/mro_monday_to_slack.git
cd mro_monday_to_slack
```

### 2. Create a Virtual Environment

It is recommended to use a virtual environment to manage the project dependencies.

```bash
python3 -m venv venv
source venv/bin/activate
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
