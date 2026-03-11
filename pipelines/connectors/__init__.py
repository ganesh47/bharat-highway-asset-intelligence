from .base import Connector
from .datagovin_ogd import DataGovInConnector
from .morth_annual_report import MoRTHAnnualReportConnector
from .nhai_publications import NHAIPublicationConnector
from .nhai_annual_documents import NHAIAnnualDocumentsConnector
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
from .model_panels import HighwayProjectRiskPanelConnector

CONNECTORS = [
    DataGovInConnector(),
    MoRTHAnnualReportConnector(),
    NHAIAnnualDocumentsConnector(),
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
    HighwayProjectRiskPanelConnector(),
]

__all__ = ["Connector", "CONNECTORS"]
