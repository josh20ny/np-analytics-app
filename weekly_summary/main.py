# weekly_summary/main.py

from weekly_summary.data_access import fetch_all_with_yoy
from weekly_summary.formatter import format_summary
from weekly_summary.clickup_client import post_message


def run():
    """
    Fetch current and prior-year data, format the summary, and post to ClickUp.
    """
    data = fetch_all_with_yoy()
    message = format_summary(data)
    post_message(message)
    print("✅ Weekly snapshot sent!")


if __name__ == "__main__":
    run()
