"""Ingest SSP population projection data into Bronze.

SSP (Shared Socioeconomic Pathways) county-level projection data is not
available via a public API. The underlying dataset must be manually
downloaded from the IIASA SSP Database:
  https://tntcat.iiasa.ac.at/SspDb/

Or for US county-level SSP projections:
  https://sedac.ciesin.columbia.edu/data/set/popdynamics-us-county-level-ssp
"""

import logging

logger = logging.getLogger(__name__)


def ingest(spark, scenario: str = "SSP2", catalog: str | None = None):
    """Ingest SSP projection data into Bronze.

    Raises NotImplementedError because SSP projection data requires
    manual download from the IIASA database or SEDAC and cannot be
    fetched via a public API.
    """
    raise NotImplementedError(
        "SSP projection data requires manual download from the IIASA SSP Database "
        "(https://tntcat.iiasa.ac.at/SspDb/) or SEDAC "
        "(https://sedac.ciesin.columbia.edu/data/set/popdynamics-us-county-level-ssp). "
        f"Requested scenario: {scenario}. "
        "Download the dataset, place it in a staging location, and implement "
        "a file-based parser for ingestion."
    )
