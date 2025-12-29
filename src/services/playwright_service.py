import time
import logging
import os
from playwright.sync_api import sync_playwright

AUTH_FILE = "auth.json"


class PlaywrightService:
    def __init__(self, headless=True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.context = None

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)

        if os.path.exists(AUTH_FILE):
            logging.info(f"Loading authentication state from {AUTH_FILE}")
            self.context = self.browser.new_context(storage_state=AUTH_FILE)
        else:
            logging.warning(
                f"Auth file {AUTH_FILE} not found. You may need to authenticate first.")
            self.context = self.browser.new_context()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def authenticate(self):
        """Runs browser in headful mode to allow manual login and saves state."""
        logging.info(
            "Starting authentication process. Please log in manually in the browser window.")
        # Force headless=False for auth
        if self.browser:
            self.browser.close()

        self.browser = self.playwright.chromium.launch(headless=False)
        self.context = self.browser.new_context()
        page = self.context.new_page()

        page.goto("https://mrosupply-team.monday.com/")

        try:
            # Wait for a specific element that indicates we are logged in (e.g., user avatar or main menu)
            # Waiting up to 5 minutes for user to login
            page.wait_for_selector('div[class*="avatar"]', timeout=300000)
            logging.info("Login detected!")

            # Save state
            self.context.storage_state(path=AUTH_FILE)
            logging.info(f"Authentication state saved to {AUTH_FILE}")

        except Exception as e:
            logging.error(f"Authentication failed or timed out: {e}")

    def download_markdown(self, url, download_dir="downloads"):
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        page = self.context.new_page()
        try:
            logging.info(f"Navigating to: {url}")
            page.goto(url)

            # Ждем, пока прекратится сетевая активность (загрузка данных документа)
            try:
                page.wait_for_load_state("networkidle", timeout=60000)
            except Exception:
                logging.warning(
                    "Network idle timeout (background tasks?), proceeding...")

            if "login" in page.url:
                logging.error(
                    "Redirected to login page. Session likely expired. Please run with --auth again.")
                return None

            # 1. Wait for the modal to appear (Monday opens docs in a modal overlay)
            logging.info("Waiting for document modal...")
            modal = page.locator('.ReactModal__Content')
            modal.wait_for(state="visible", timeout=60000)

            # 2. Wait for the document content to load inside the modal
            logging.info("Waiting for document content to render...")
            modal.locator(
                '#editor-content').wait_for(state="visible", timeout=60000)

            # 3. Find the menu button specifically INSIDE the modal
            menu_button = modal.locator(
                '#doc-in-file-more-actions-button, [aria-label="Options"]').first
            menu_button.wait_for(timeout=60000)

            # Click menu
            menu_button.click()
            # Small pause to ensure menu animation starts
            page.wait_for_timeout(2000)

            # Click Export
            # Wait for Export option to be visible
            # Use role and aria-disabled to avoid clicking the disabled "Export as" header in the submenu
            export_option = page.locator(
                '[role="menuitem"]:not([aria-disabled="true"]) span[data-testid="menu-item-title"]'
            ).filter(has_text="Export").first
            export_option.wait_for(state="visible", timeout=30000)

            # Hover to trigger submenu
            export_option.hover()
            page.wait_for_timeout(1000)

            # Define Markdown option selector (using role=menuitem for better hit target)
            md_option = page.locator('[role="menuitem"]').filter(
                has_text="Markdown (.md)").first

            # If submenu didn't appear on hover, click the Export button
            if not md_option.is_visible():
                export_option.click()
                page.wait_for_timeout(1000)

            # Click Markdown and handle download
            # Increased timeout to 120s because generation can be slow (client-side JS)
            with page.expect_download(timeout=120000) as download_info:
                md_option.wait_for(state="visible", timeout=30000)
                # Explicit pause to ensure UI is ready before click
                page.wait_for_timeout(1000)
                md_option.click()

            download = download_info.value
            # Use original filename suggested by Monday
            original_name = download.suggested_filename
            file_path = os.path.join(download_dir, original_name)
            download.save_as(file_path)

            logging.info(f"Downloaded: {file_path}")
            return file_path

        except Exception as e:
            logging.error(f"Failed to download MD from {url}: {e}")
            try:
                page.screenshot(
                    path=f"temp_dev/error_screenshot_{int(time.time())}.png")
                logging.info("Saved error screenshot.")
            except Exception:
                pass
            return None
        finally:
            page.close()
