from abc import ABC, abstractmethod
from domain.models.notification import Notification

class NotificationPort(ABC):
    @abstractmethod
    def notify(self, messages: list[Notification]) -> None:
        ...
    