"""Gold scoring job — join all Silver tables, normalize, score, write Gold."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, struct, lit, udf, row_number, coalesce, when,
    count as spark_count,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

from src.common.config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA, DEFAULT_WEIGHTS, MIN_SOURCES_FOR_SCORE
from src.common.fips import fips_to_state_abbr
from src.gold.scoring import compute_composite_score, assign_tier


def _try_table(spark, table_name):
    """Try to read a Silver table; return None if it doesn't exist."""
    import logging
    try:
        df = spark.table(table_name)
        # Force schema resolution to catch TABLE_NOT_FOUND eagerly
        _ = df.columns
        return df
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e):
            logging.getLogger(__name__).warning(f"Skipping missing table: {table_name}")
            return None
        raise


def _latest_per_fips(df: DataFrame, cols: list[str]) -> DataFrame:
    """Keep only the most recent row per FIPS (by report_year, report_quarter)."""
    order_cols = []
    if "report_year" in df.columns:
        order_cols.append(col("report_year").desc())
    if "report_quarter" in df.columns:
        order_cols.append(col("report_quarter").desc())
    if not order_cols:
        return df.select("fips", *cols)

    w = Window.partitionBy("fips").orderBy(*order_cols)
    return (
        df.withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select("fips", *cols)
    )


def build_indicator_table(spark: SparkSession, catalog: str = CATALOG) -> DataFrame:
    """Join all Silver tables into one row per county using latest available data."""
    silver = f"{catalog}.{SILVER_SCHEMA}"

    # --- Load each source and keep latest row per county ---
    base = _latest_per_fips(
        spark.table(f"{silver}.silver_acs_demographics"),
        ["report_year", "population", "median_income"],
    ).withColumnRenamed("report_year", "acs_year")

    permits_raw = spark.table(f"{silver}.silver_building_permits")
    permits = _latest_per_fips(permits_raw, [
        "county_name", "report_year", "report_quarter",
        "total_units_permitted", "single_family_units", "multi_family_units",
    ])

    migration_raw = _try_table(spark, f"{silver}.silver_migration")
    migration = (
        _latest_per_fips(migration_raw, ["net_migration", "net_migration_rate"])
        if migration_raw else None
    )

    vacancy_raw = _try_table(spark, f"{silver}.silver_vacancy")
    vacancy = (
        _latest_per_fips(vacancy_raw, ["vacancy_rate"])
        if vacancy_raw else None
    )

    employment_raw = _try_table(spark, f"{silver}.silver_employment")
    employment = (
        _latest_per_fips(employment_raw, ["total_employment", "avg_weekly_wage"])
        if employment_raw else None
    )

    school_raw = _try_table(spark, f"{silver}.silver_school_enrollment")
    school = (
        _latest_per_fips(school_raw, ["total_enrollment"])
        if school_raw else None
    )

    business_raw = _try_table(spark, f"{silver}.silver_business_patterns")
    business = (
        _latest_per_fips(business_raw, ["qsr_establishments", "retail_density"])
        if business_raw else None
    )

    ssp_raw = _try_table(spark, f"{silver}.silver_ssp_projections")
    ssp = None
    if ssp_raw is not None:
        from pyspark.sql.functions import substring, max as spark_max
        # SSP data is state-level (state_fips in fips column or 2-digit codes)
        # Get the latest projection year per state for SSP2 scenario
        ssp_filtered = ssp_raw.filter(col("scenario") == "SSP2")
        # Determine if fips is 2-digit state or 5-digit county
        ssp_cols = ssp_filtered.columns
        if "state_fips" in ssp_cols:
            ssp_state_col = col("state_fips")
        else:
            # fips might be 2-digit state codes
            ssp_state_col = col("fips")
        # Get max projection year per state for the growth rate
        ssp_latest = ssp_filtered.withColumn("_state", ssp_state_col)
        w_ssp = Window.partitionBy("_state").orderBy(col("projection_year").desc())
        ssp_latest = (
            ssp_latest.withColumn("_rn", row_number().over(w_ssp))
            .filter(col("_rn") == 1)
            .drop("_rn")
            .select(
                col("_state").alias("_ssp_state"),
                col("projected_population").alias("ssp_projected_pop"),
                col("projection_year").alias("ssp_projection_year"),
                coalesce(col("projected_growth_rate"), lit(0.0)).alias("ssp_growth_rate"),
            )
        )
        ssp = ssp_latest

    # --- Join everything on fips only ---
    combined = permits.join(base, on="fips", how="outer")
    for df in [migration, vacancy, employment, school, business]:
        if df is not None:
            combined = combined.join(df, on="fips", how="left")

    # SSP joins on state prefix (first 2 digits of FIPS)
    if ssp is not None:
        from pyspark.sql.functions import substring
        combined = combined.withColumn("_state_fips", substring(col("fips"), 1, 2))
        combined = combined.join(ssp, combined["_state_fips"] == ssp["_ssp_state"], "left")
        combined = combined.drop("_state_fips", "_ssp_state")

    state_udf = udf(fips_to_state_abbr, StringType())
    combined = combined.withColumn("state", state_udf(col("fips")))

    combined = combined.withColumn(
        "permits_per_1k_pop",
        coalesce(col("total_units_permitted"), lit(0)) / col("population") * 1000
    )

    # Compute per-capita metrics from raw values
    if "total_employment" in combined.columns:
        combined = combined.withColumn(
            "employment_per_capita",
            col("total_employment") / col("population")
        )
    if "total_enrollment" in combined.columns:
        combined = combined.withColumn(
            "enrollment_per_capita",
            col("total_enrollment") / col("population")
        )
    # Invert vacancy rate: lower vacancy = better for store siting
    if "vacancy_rate" in combined.columns:
        combined = combined.withColumn(
            "occupancy_rate",
            lit(1.0) - coalesce(col("vacancy_rate"), lit(0.0))
        )

    return combined


def score_counties(indicator_df: DataFrame, weights: dict | None = None) -> DataFrame:
    """Normalize indicators and compute composite scores.

    Uses adaptive weighting: if an indicator is NULL for a county, its weight
    is redistributed proportionally among available indicators. This prevents
    missing data from dragging scores to zero.
    """
    w = weights or DEFAULT_WEIGHTS

    from pyspark.sql.functions import min as spark_min, max as spark_max, greatest

    indicator_cols = {
        "building_permits": "permits_per_1k_pop",
        "net_migration": "net_migration_rate",
        "vacancy_change": "occupancy_rate",
        "employment_growth": "employment_per_capita",
        "school_enrollment_growth": "enrollment_per_capita",
        "ssp_projected_growth": "ssp_growth_rate",
    }

    # These indicators are inverted: higher raw value = lower score
    inverted = set()

    df = indicator_df
    available_cols = set(df.columns)

    for indicator_name, source_col in indicator_cols.items():
        has_col = f"_has_{indicator_name}"
        norm_col = f"_norm_{indicator_name}"

        if source_col not in available_cols:
            df = df.withColumn(norm_col, lit(None).cast(DoubleType()))
            df = df.withColumn(has_col, lit(0.0))
            continue

        min_col = f"_min_{indicator_name}"
        max_col = f"_max_{indicator_name}"

        full_window = Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)
        df = df.withColumn(min_col, spark_min(col(source_col)).over(full_window))
        df = df.withColumn(max_col, spark_max(col(source_col)).over(full_window))

        range_expr = col(max_col) - col(min_col)
        normalized = when(
            col(source_col).isNull(), lit(None).cast(DoubleType())
        ).when(
            range_expr == 0, lit(0.5)
        ).otherwise(
            (col(source_col) - col(min_col)) / range_expr
        )

        if indicator_name in inverted:
            normalized = when(normalized.isNull(), lit(None)).otherwise(lit(1.0) - normalized)

        df = df.withColumn(norm_col, normalized)
        # Track which indicators are available per row
        df = df.withColumn(has_col, when(col(source_col).isNotNull(), lit(w[indicator_name])).otherwise(lit(0.0)))
        df = df.drop(min_col, max_col)

    # Adaptive weighting: sum of available weights per row, then rescale
    available_weight_expr = sum(col(f"_has_{name}") for name in w)
    df = df.withColumn("_available_weight", greatest(available_weight_expr, lit(0.01)))

    # Score = sum(norm * weight / available_weight) * 100
    score_expr = sum(
        coalesce(col(f"_norm_{name}"), lit(0.0)) * lit(w[name])
        for name in w
    ) / col("_available_weight") * 100

    df = df.withColumn("composite_score", score_expr)

    # Use percentile-based tier assignment via ntile
    from pyspark.sql.functions import ntile
    tier_window = Window.orderBy(col("composite_score").desc())
    df = df.withColumn("_pctile", ntile(100).over(tier_window))
    df = df.withColumn("score_tier",
        when(col("_pctile") <= 10, lit("A"))
        .when(col("_pctile") <= 30, lit("B"))
        .when(col("_pctile") <= 60, lit("C"))
        .when(col("_pctile") <= 85, lit("D"))
        .otherwise(lit("F"))
    )
    df = df.drop("_pctile")

    rank_window = Window.orderBy(col("composite_score").desc())
    df = df.withColumn("rank_national", row_number().over(rank_window))

    df = df.withColumn("component_scores", struct(
        *[coalesce(col(f"_norm_{name}"), lit(0.0)).alias(name) for name in w]
    ))

    for name in w:
        df = df.drop(f"_norm_{name}")
        df = df.drop(f"_has_{name}")
    df = df.drop("_available_weight")

    return df


def run_gold_scoring(spark: SparkSession, catalog: str = CATALOG):
    """Main entry point for Gold scoring job."""
    indicator_df = build_indicator_table(spark, catalog)
    scored_df = score_counties(indicator_df)

    score_cols = [
        "fips", "county_name", "state", "report_year", "report_quarter",
        "population", "median_income",
        "composite_score", "score_tier", "rank_national",
        "component_scores",
        "ssp_projected_pop", "ssp_projection_year", "ssp_growth_rate",
    ]
    # Only select columns that exist (some may be missing if sources are unavailable)
    available = set(scored_df.columns)
    select_cols = [c for c in score_cols if c in available]
    scored_df.select(*select_cols).write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{catalog}.{GOLD_SCHEMA}.gold_county_growth_score")

    indicator_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{GOLD_SCHEMA}.gold_county_details")

    from pyspark.sql import Row
    config_rows = [Row(indicator=k, weight=v) for k, v in DEFAULT_WEIGHTS.items()]
    spark.createDataFrame(config_rows).write.mode("overwrite").saveAsTable(
        f"{catalog}.{GOLD_SCHEMA}.gold_scoring_config"
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold-scoring").getOrCreate()
    run_gold_scoring(spark)
