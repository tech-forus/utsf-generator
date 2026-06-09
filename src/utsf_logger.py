import threading
from datetime import datetime
from typing import Dict, List, Any

class UTSFLogger:
    def __init__(self):
        self._local = threading.local()

    def init_logs(self):
        self._local.logs = []

    def log_stage(self, stage: str, message: str, data: Dict[str, Any] = None):
        if not hasattr(self._local, "logs"):
            self._local.logs = []
        
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "stage": stage,
            "message": message,
            "data": data or {}
        }
        self._local.logs.append(entry)
        print(f"[{stage}] {message}")

    def get_logs(self) -> List[Dict[str, Any]]:
        return getattr(self._local, "logs", [])

utsf_logger = UTSFLogger()
