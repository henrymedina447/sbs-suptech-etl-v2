import logging
import random
import time
from typing import Any, Callable, TypeVar
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError, EndpointConnectionError, ConnectionClosedError, \
    BotoCoreError
from langchain_aws import ChatBedrockConverse
from pydantic import SecretStr, BaseModel
from application.ports.transform_document_port import TransformDocumentPort
from domain.models.states.etl_inscripciones_state import EtlInscripcionesState, EtlInscripcionChild
from domain.models.states.etl_polizas_state import EtlPolizasState
from domain.models.states.etl_tasaciones_state import EtlTasacionesState
from infrastructure.config.app_settings import AppSettings, get_app_settings

T = TypeVar("T", bound=BaseModel)


class BedRockTransformerDocument(TransformDocumentPort):
    def __init__(self):
        self._app_settings: AppSettings = get_app_settings()
        self.bedrock_converse: ChatBedrockConverse = self._get_bedrock()

    def _get_bedrock(self) -> ChatBedrockConverse:
        retries_config: Any = {"max_attempts": 12, "mode": "adaptive"}
        config = Config(
            retries=retries_config,
            read_timeout=300,
            connect_timeout=10,
            tcp_keepalive=True,
            max_pool_connections=50
        )
        client = boto3.client("bedrock-runtime", region_name=self._app_settings.aws_settings.region, config=config)
        return ChatBedrockConverse(
            model="anthropic.claude-3-5-sonnet-20240620-v1:0",
            region_name=self._app_settings.aws_settings.region,
            bedrock_client=client,
        )

    # ------ Pólizas
    def llm_caller_polizas(self, context: str) -> EtlPolizasState | None:
        return BedRockTransformerDocument.with_throttling_retry(self._llm_polizas_internal_chain, context)

    def _llm_polizas_internal_chain(self, context: str) -> EtlPolizasState:
        poliza_system_prompt = """Eres un experto obteniendo información de las pólizas; donde debes analizar la 
        entrada del usuario y devolver el número de la póliza (la puedes encontrar por general cerca a la palabra 
        póliza), el nombre de quien está la póliza (razón social, por lo general cerca a palabras de Nombre o datos 
        del contratante), la fecha de inicio( puede estar cerca de palabras como fecha de inicio o vigencia desde) y 
        la fecha fin de la póliza (puede estar como fecha fin o fin o hasta); debes entregar tus hallazgos en el 
        siguiente formato: { "policy_number": "Número de la póliza", "policy_name": "A nombre de quien está la 
        póliza, la razón social", "policy_start_date": "Fecha de inicio de la póliza", "policy_end_date": "Fecha fin 
        de la póliza" }"""
        messages = [
            ("system", f"{poliza_system_prompt}"),
            ("human", f"{context}")
        ]
        chain = self.bedrock_converse.with_structured_output(EtlPolizasState)
        results = chain.invoke(messages)
        return EtlPolizasState.model_validate(results)

    # ----- Inscripciones
    def llm_caller_inscripciones(self, context: str) -> EtlInscripcionChild | None:
        return BedRockTransformerDocument.with_throttling_retry(self._llm_inscripciones_internal_chain, context)

    def _llm_inscripciones_internal_chain(self, context: str) -> EtlInscripcionChild:
        inscripciones_system_prompt = """Eres un experto obteniendo información de las inscripciones (números de 
        partidas) que se hace en SUNARP; donde debes analizar el contexto y obtener el número de inscripción (también 
        conocido como número de partida), a favor de quien está a favor de quien está la inscripción (por lo general 
        lo puedes encontrar cerca de acreedor hipotecario), luego debes obtener la fecha de inscripción (por lo 
        general cerca a textos como "el titulo fue presenta el 06/12/2021 a las 08:18:43 AM"); tus hallazgos los 
        debes retornar en el siguiente formato: { inscription_number: Hace referencia al número de inscripción, 
        legal_name: Hace referencia a la razón social / beneficiario de la inscripción, inscription_date: Hace 
        referencia a la fecha en la que se realizó la inscripción}"""
        messages = [
            ("system", f"{inscripciones_system_prompt}"),
            ("human", f"{context}")
        ]
        chain = self.bedrock_converse.with_structured_output(EtlInscripcionChild)
        results = chain.invoke(messages)
        return EtlInscripcionChild.model_validate(results)

    # --- Tasaciones
    def llm_caller_tasaciones(self, context: str) -> EtlTasacionesState | None:
        return BedRockTransformerDocument.with_throttling_retry(self._llm_tasaciones_internal_chain, context)

    def _llm_tasaciones_internal_chain(self, context: str) -> EtlTasacionesState:
        tasacion_system_prompt = """Eres un experto obteniendo información de las tasaciones, para lo cual debes 
        obtener información del nombre del perito (el cual por lo general lo encuentra como perito evaluador o perito 
        y tambien sobre palabras de ing. o lic), la fecha de la tasación (por lo general cerca a la palabra fecha); 
        el valor comercial en soles (puede estar alrededor de palabras como Valor comercial (VC) SOLES S/.) y el 
        valor de realización en soles igual puede estar alrededor de palabras como Valor de realización (VR) SOLES 
        S/; también debes obtener al propietario de la tasación por lo general esta alrededor de la palabra 
        "propietario" o "propietaria" tus hallazgos los debes retornar en json de la siguiente forma: { 
        "expert_warranty_name": "Indica el nombre del perito", "tasacion_date": "Indica la fecha de la tasación",
        retornalo en formato dd/mm/aaaa", "commercial_value": "Indica el valor comercial en soles ( PEN)", 
        "realization_value": "Indica el valor de realización en soles (PEN)", "tasacion_owner": "Indica el nombre del 
        propietario de la tasación" } ;"""
        messages = [
            ("system", f"{tasacion_system_prompt}"),
            ("human", f"{context}")
        ]
        chain = self.bedrock_converse.with_structured_output(EtlTasacionesState)
        results = chain.invoke(messages)
        return EtlTasacionesState.model_validate(results)

    @staticmethod
    def with_throttling_retry(func: Callable[..., T], *args, max_retries=5, backoff_base=1.0, backoff_factor=2.0,
                              max_backoff=30.0,
                              **kwargs):
        """
        Ejecuta func(*args, **kwargs) con manejo de ThrottlingException y backoff.

        Parámetros:
            func: función a ejecutar
            *args, **kwargs: argumentos que recibe la función
            max_retries: máximo de reintentos
            backoff_base: segundos de espera inicial
            backoff_factor: multiplicador exponencial
            max_backoff: límite máximo de espera en segundos
        """
        retries = 0
        while True:
            try:
                return func(*args, **kwargs)

            except ClientError as e:
                code = (e.response or {}).get("Error", {}).get("Code", "")
                if code == "ThrottlingException" and retries < max_retries:
                    wait = min(backoff_base * (backoff_factor ** retries) + random.uniform(0, 1), max_backoff)
                    print(
                        f"[Retry] Throttling detectado. Esperando {wait:.2f}s antes de reintentar ({retries + 1}/{max_retries})")
                    time.sleep(wait)
                    retries += 1
                    continue
                raise  # si no es throttling o ya agotó reintentos, lo lanzo
            except (ReadTimeoutError, EndpointConnectionError, ConnectionClosedError, TimeoutError, BotoCoreError) as e:
                if retries < max_retries:
                    wait = min(backoff_base * (backoff_factor ** retries) + random.uniform(0, 1), max_backoff)
                    logging.warning(
                        f"[Retry] Error transitorio '{e.__class__.__name__}'. Esperando {wait:.2f}s ({retries + 1}/{max_retries})")
                    time.sleep(wait)
                    retries += 1
                    continue
                raise
