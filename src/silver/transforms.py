"""Master Silver transform job — reads all Bronze tables, writes Silver."""

from pyspark.sql import SparkSession
from src.common.config import CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA
from src.silver.building_permits import transform_building_permits
from src.silver.migration import transform_migration
from src.silver.vacancy import transform_vacancy
from src.silver.hud_construction import transform_hud_construction
from src.silver.employment import transform_employment
from src.silver.school_enrollment import transform_school_enrollment
from src.silver.acs_demographics import transform_acs_demographics
from src.silver.business_patterns import transform_business_patterns
from src.silver.ssp_projections import transform_ssp_projections
from src.silver.national_projections import transform_national_projections
from src.silver.overture_poi import transform_overture_poi
from src.silver.overture_segments import transform_overture_segments


def run_all_silver_transforms(spark: SparkSession, catalog: str = CATALOG):
    """Execute all Silver transforms and write to Unity Catalog."""
    bronze = f"{catalog}.{BRONZE_SCHEMA}"
    silver = f"{catalog}.{SILVER_SCHEMA}"

    # Phase 1: ACS demographics must run first (provides pop_df for others)
    acs_df = transform_acs_demographics(spark.table(f"{bronze}.acs_demographics"))
    acs_df.write.mode("overwrite").saveAsTable(f"{silver}.silver_acs_demographics")

    pop_df = acs_df.select("fips", "population")

    # Phase 2: All other transforms (no interdependencies)
    transforms = [
        ("building_permits", lambda: transform_building_permits(
            spark.table(f"{bronze}.building_permits"))),
        ("migration", lambda: transform_migration(
            spark.table(f"{bronze}.migration"), pop_df)),
        ("vacancy", lambda: transform_vacancy(
            spark.table(f"{bronze}.vacancy"))),
        ("hud_construction", lambda: transform_hud_construction(
            spark.table(f"{bronze}.hud_construction"))),
        ("employment", lambda: transform_employment(
            spark.table(f"{bronze}.employment"))),
        ("school_enrollment", lambda: transform_school_enrollment(
            spark.table(f"{bronze}.school_enrollment"))),
        ("business_patterns", lambda: transform_business_patterns(
            spark.table(f"{bronze}.business_patterns"), pop_df)),
        ("ssp_projections", lambda: transform_ssp_projections(
            spark.table(f"{bronze}.ssp_projections"))),
        ("national_projections", lambda: transform_national_projections(
            spark.table(f"{bronze}.national_projections"))),
        ("poi_restaurants", lambda: transform_overture_poi(
            spark.table(f"{bronze}.overture_places"))),
        ("road_segments", lambda: transform_overture_segments(
            spark.table(f"{bronze}.overture_segments"))),
    ]

    import logging
    logger = logging.getLogger(__name__)

    for name, transform_fn in transforms:
        try:
            df = transform_fn()
            df.write.mode("overwrite").saveAsTable(f"{silver}.silver_{name}")
            logger.info(f"Silver transform {name} completed successfully")
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e):
                logger.warning(f"Skipping {name}: bronze table not found ({e})")
            else:
                raise


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver-transforms").getOrCreate()
    run_all_silver_transforms(spark)
