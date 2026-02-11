from __future__ import annotations

from prep_brain.config import load_config
from prep_brain.logging import configure_logging
from prep_brain.telegram.app import run


def main() -> None:
    config = load_config()
    configure_logging(config)
    run()


if __name__ == "__main__":
    main()
