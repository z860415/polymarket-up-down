"""
正式版檔案日誌配置。

將正式運行的關鍵事件分流到獨立檔案，便於 VPS 常駐追查。
"""

from __future__ import annotations

import logging
from pathlib import Path

LOG_CATEGORIES = (
    "lifecycle",
    "preflight",
    "candidate",
    "order",
    "fill",
    "claim",
    "error",
)


class _CategoryFilter(logging.Filter):
    """只允許指定分類 logger 寫入目標檔案。"""

    def __init__(self, category: str) -> None:
        super().__init__()
        self.category = category

    def filter(self, record: logging.LogRecord) -> bool:
        """只接受指定分類與其子 logger。"""
        target = f"polymarket.{self.category}"
        return record.name == target or record.name.startswith(f"{target}.")


class _ErrorFilter(logging.Filter):
    """收斂 error 類事件。"""

    def filter(self, record: logging.LogRecord) -> bool:
        """接受 error 分類或 ERROR 級別以上事件。"""
        return record.levelno >= logging.ERROR or record.name.startswith("polymarket.error")


def configure_application_logging(log_dir: str, level: str = "INFO") -> Path:
    """配置終端與正式版檔案日誌。"""
    directory = Path(log_dir).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(console_handler)

    # 第三方 HTTP 客戶端預設降噪，避免正式版常駐時刷滿檔案。
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    for category in LOG_CATEGORIES:
        file_handler = logging.FileHandler(directory / f"{category}.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        if category == "error":
            file_handler.addFilter(_ErrorFilter())
        else:
            file_handler.addFilter(_CategoryFilter(category))
        root.addHandler(file_handler)

    return directory
