from abc import ABC, abstractmethod


class LoaderDocumentPort(ABC):
    @abstractmethod
    def save_document(self, key: str, data: bytes) -> None:
        ...
