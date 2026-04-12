from enum import Enum


class ComponentType(str, Enum):
    MATERIAL = "material"
    PACKAGING = "packaging"
    FICTITIOUS_LABOR = "fictitious_labor"
    FICTITIOUS_OVERHEAD = "fictitious_overhead"
    FICTITIOUS_OTHER = "fictitious_other"
    UNKNOWN = "unknown"


class ProcessType(str, Enum):
    DEHYDRATION = "dehydration"
    GRINDING = "grinding"
    MIXING = "mixing"
    PACKAGING = "packaging"
