from abc import ABC, abstractmethod
from domain.models.states.etl_base_state import EtlBaseState


class LoaderMetadataPort(ABC):
    @abstractmethod
    def save_metadata(self, document_type: str, data: list[EtlBaseState]) -> None:
        ...
