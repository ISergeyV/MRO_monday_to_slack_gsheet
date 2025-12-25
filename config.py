import os
from dotenv import load_dotenv

load_dotenv()

MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
MONDAY_API_URL = "https://api.monday.com/v2"
MONDAY_BOARD_ID = os.getenv("MONDAY_BOARD_ID")

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']

SHEET_ID = os.getenv("SHEET_ID")

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

STATE_FILE = "migration_state.txt"
