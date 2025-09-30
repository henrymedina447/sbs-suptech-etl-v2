from abc import ABC, abstractmethod
from typing import Any

from langgraph.constants import START, END
from langgraph.graph import StateGraph

from application.ports.loader_metadata_port import LoaderMetadataPort
from application.ports.loader_document_port import LoaderDocumentPort
from application.ports.transform_document_port import TransformDocumentPort
from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.notification_port import NotificationPort
from domain.models.states.etl_base_state import EtlBaseState


class WorkflowBase(ABC):
    def __init__(self,
                 extractor: ExtractorDocumentPort,
                 transformer: TransformDocumentPort,
                 metadata_loader: LoaderMetadataPort,
                 document_loader: LoaderDocumentPort
                 ):
        self._extractor = extractor
        self._transformer = transformer
        self._metadata_loader = metadata_loader
        self._document_loader = document_loader
        self._graph = self._build_graph()

    @abstractmethod
    def _extract(self, state: EtlBaseState) -> dict[str, Any]:
        ...

    @abstractmethod
    def _transform(self, state: EtlBaseState) -> dict[str, Any]:
        ...

    @abstractmethod
    def _load(self, state: EtlBaseState) -> dict[str, Any]:
        ...

    @abstractmethod
    def _final_task(self, state: EtlBaseState) -> dict[str, Any]:
        ...

    def _build_graph(self):
        g = StateGraph(EtlBaseState)
        g.add_node("extract", self._extract)
        g.add_node("transform", self._transform)
        g.add_node("load", self._load)
        g.add_node("final_task", self._final_task)
        g.add_edge(START, "extract")
        g.add_edge("extract", "transform")
        g.add_edge("transform", "load")
        g.add_edge("load", "final_task")
        g.add_edge("final_task", END)
        return g.compile()
