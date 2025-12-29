import requests
import logging
import config

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(message)s')


def get_board_columns():
    query = f"""
    query {{
      boards (ids: {config.MONDAY_BOARD_ID}) {{
        name
        columns {{
          id
          title
          type
        }}
      }}
    }}
    """

    headers = {"Authorization": config.MONDAY_API_KEY}
    try:
        response = requests.post(config.MONDAY_API_URL, json={
                                 'query': query}, headers=headers)
        response.raise_for_status()
        data = response.json()

        if 'errors' in data:
            logging.error(f"GraphQL Errors: {data['errors']}")
            return

        boards = data.get('data', {}).get('boards', [])
        if not boards:
            logging.warning("No boards found.")
            return

        board = boards[0]
        logging.info(f"Board: {board['name']}")
        logging.info("-" * 60)
        logging.info(f"{'ID':<25} | {'Type':<15} | {'Title'}")
        logging.info("-" * 60)
        for col in board['columns']:
            logging.info(
                f"{col['id']:<25} | {col['type']:<15} | {col['title']}")

    except Exception as e:
        logging.error(f"Request failed: {e}")


if __name__ == "__main__":
    get_board_columns()
