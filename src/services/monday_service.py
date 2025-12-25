import requests
import logging
import time
import config


class CursorExpiredException(Exception):
    pass


def fetch_monday_page(cursor=None, include_assets=True):
    variables = {}

    # Запрашиваем assets только если они нужны (экономия трафика при пропуске)
    assets_query = """
              assets {
                id
                name
                public_url
                file_extension
              }
    """ if include_assets else ""

    if cursor:
        query = f"""
        query ($cursor: String!) {{
          next_items_page (cursor: $cursor, limit: 25) {{
            cursor
            items {{
              id
              name
              {assets_query}
            }}
          }}
        }}
        """
        variables['cursor'] = cursor
    else:
        query = f"""
        query {{
          boards (ids: {config.MONDAY_BOARD_ID}) {{
            items_page (limit: 25) {{
              cursor
              items {{
                id
                name
                {assets_query}
              }}
            }}
          }}
        }}
        """

    headers = {"Authorization": config.MONDAY_API_KEY}

    for attempt in range(3):
        try:
            response = requests.post(config.MONDAY_API_URL, json={
                                     'query': query, 'variables': variables}, headers=headers)
            response.raise_for_status()
            json_response = response.json()

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
            time.sleep(2 ** attempt)

    logging.error("Failed to fetch data from Monday.com after 3 attempts.")
    return [], None


def fetch_monday_items_generator(start_item=1):
    cursor = None
    items_processed = 0

    while True:
        # Если мы еще далеко от start_item, не запрашиваем assets (быстрая перемотка)
        # +25 с запасом, так как страница может быть неполной
        include_assets = (items_processed + 25 >= start_item)

        items, cursor = fetch_monday_page(
            cursor, include_assets=include_assets)

        if items:
            logging.info(
                f"Fetched page with {len(items)} items. (Processed so far: {items_processed})")
            for item in items:
                items_processed += 1
                if items_processed >= start_item:
                    yield item
        if not cursor:
            break
