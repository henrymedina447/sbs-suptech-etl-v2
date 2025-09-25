from enum import Enum


class PrefixEnum(str, Enum):
    polizas = "Polizas/"
    inscripciones = "Inscripciones/"
    tasaciones = "Tasaciones/"
