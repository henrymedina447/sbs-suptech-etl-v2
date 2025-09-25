from abc import ABC, abstractmethod

from domain.models.states.etl_base_state import EtlBaseState


class ExtractorDocumentPort(ABC):

    @abstractmethod
    def extract_pipeline(self, **kwargs) -> EtlBaseState | None:
        ...
