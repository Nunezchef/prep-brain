import requests
from dashboard.utils import load_config

def send_telegram_notification(message: str):
    """
    Sends a message securely via the Telegram Bot API using requests (synchronous).
    Best for dashboard use where no async loop is running.
    """
    config = load_config()
    token = config.get("telegram", {}).get("token")
    # We need a chat_id. Since the bot might talk to multiple people, 
    # we ideally want to notify the *admin* or the specific user.
    # For this MVP, we will try to find a 'default_admin_id' or 'allowed_users' in config.
    # If not found, we can't send.
    
    # Let's assume the first allowed user is the admin for now if no explicit admin_id
    allowed_users = config.get("telegram", {}).get("allowed_users", [])
    if not token or not allowed_users:
        print("Notifier: No token or allowed users found.")
        return False
        
    # Send to all allowed users? Or just the first?
    # Let's send to all allowed users for visibility.
    success_count = 0
    for user_id in allowed_users:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": user_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                success_count += 1
            else:
                print(f"Notifier Error {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"Notifier Exception: {e}")
            
    return success_count > 0
