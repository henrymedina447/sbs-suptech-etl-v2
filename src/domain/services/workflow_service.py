import re

from domain.models.states.etl_base_state import EtlBaseState
from domain.models.states.etl_inscripciones_state import EtlInscripcionesState, EtlInscripcionChild


class WorkflowService:
    @staticmethod
    def refine_dates(date_str: str) -> str | None:
        """Extrae dd/mm/aaaa de un string; si viene None o no hay match, 'no value'."""
        if not date_str:
            return None
        m = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", date_str)
        return m.group(1) if m else None

    @staticmethod
    def refine_month(month: str | None) -> str | None:
        month_es: dict[str, int] = {
            "enero": 1,
            "febrero": 2,
            "marzo": 3,
            "abril": 4,
            "mayo": 5,
            "junio": 6,
            "julio": 7,
            "agosto": 8,
            "septiembre": 9,
            "octubre": 10,
            "noviembre": 11,
            "diciembre": 12,
        }

        try:
            return str(month_es.get(month.lower()))
        except KeyError as e:
            return None

    @staticmethod
    def refine_year(year: str | None) -> str | None:
        if year is None:
            return None
            # normaliza a str y valida formato simple de 4 dígitos
        y = str(year).strip()
        return y if y.isdigit() and len(y) == 4 else None

    @staticmethod
    def resolve_inscripciones_children(
            inputs: list[EtlBaseState],
            state: EtlInscripcionesState
    ) -> list[EtlInscripcionChild]:
        to_send: list[EtlInscripcionChild] = []
        for i in inputs:
            item = EtlInscripcionChild(
                record_id=state.record_id,
                period_month=state.period_month,
                period_year=state.period_year,
                extract_success=True,
                document_content_llm=i.document_content_llm,
                document_content_total=i.document_content_total
            )
            to_send.append(item)
        return to_send
