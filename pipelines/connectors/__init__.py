from .base import Connector
from .datagovin_ogd import DataGovInConnector
from .morth_annual_report import MoRTHAnnualReportConnector
from .nhai_publications import NHAIPublicationConnector
from .ncrb_accidents import NCRBAccidentsConnector
from .rbi_mospi import RBIMOSPIMacroConnector
from .stub_connectors import (
    ProcurementAwardsConnector,
    TollFastagConnector,
    QualityMaintenanceProxyConnector,
    ContractorDisclosureConnector,
    ArbitrationClaimsConnector,
    ParliamentQAConnector,
    NightlightsProxyConnector,
)

CONNECTORS = [
    DataGovInConnector(),
    MoRTHAnnualReportConnector(),
    NHAIPublicationConnector(),
    NCRBAccidentsConnector(),
    RBIMOSPIMacroConnector(),
    ProcurementAwardsConnector(),
    TollFastagConnector(),
    QualityMaintenanceProxyConnector(),
    ContractorDisclosureConnector(),
    ArbitrationClaimsConnector(),
    ParliamentQAConnector(),
    NightlightsProxyConnector(),
]

__all__ = ["Connector", "CONNECTORS"]
