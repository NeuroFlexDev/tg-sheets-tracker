from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

STATUSES = {"open", "in_progress", "done", "blocked"}
PRIORITIES = {"P0", "P1", "P2", "P3"}

@dataclass
class Task:
    id: str
    title: str
    description: str = ""
    status: str = "open"
    assignee: str = ""
    priority: str = "P2"
    due: Optional[date] = None
    labels: list[str] = None
    created_at: datetime = None
    updated_at: datetime = None
    source: str = "tg"
    tg_thread_id: Optional[int] = None
    tg_message_link: str = ""

    def __post_init__(self):
        if self.labels is None:
            self.labels = []
        now = datetime.utcnow()
        if self.created_at is None:
            self.created_at = now
        if self.updated_at is None:
            self.updated_at = now
        if self.status not in STATUSES:
            self.status = "open"
        if self.priority not in PRIORITIES:
            self.priority = "P2"
