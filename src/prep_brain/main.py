import os
from dotenv import load_dotenv

def main() -> None:
    load_dotenv()
    print("prep-brain boot OK ✅")
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    print("TELEGRAM_BOT_TOKEN set:", bool(token))

if __name__ == "__main__":
    main()