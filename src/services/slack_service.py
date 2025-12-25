import logging
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import config


def send_slack_message(item_name, drive_links):
    if not config.SLACK_TOKEN or not config.SLACK_CHANNEL:
        logging.warning(
            "Slack token or channel not configured. Skipping Slack notification.")
        return

    client = WebClient(token=config.SLACK_TOKEN)
    message = {
        "channel": config.SLACK_CHANNEL,
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
        client.chat_postMessage(**message)
        logging.info(
            f"Sent Slack notification for item: {item_name}")
    except SlackApiError as e:
        logging.error(
            f"Error sending Slack message: {e.response['error']}")
