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
    ssp = (
        _latest_per_fips(
            ssp_raw.filter(col("scenario") == "SSP2")
            .withColumnRenamed("projection_year", "report_year"),
            ["projected_population"],
        )
        if ssp_raw else None
    )

    # --- Join everything on fips only ---
    combined = permits.join(base, on="fips", how="outer")
    for df in [migration, vacancy, employment, school, business, ssp]:
        if df is not None:
            combined = combined.join(df, on="fips", how="left")

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
    """Normalize indicators and compute composite scores."""
    w = weights or DEFAULT_WEIGHTS

    from pyspark.sql.functions import min as spark_min, max as spark_max

    indicator_cols = {
        "building_permits": "permits_per_1k_pop",
        "net_migration": "net_migration_rate",
        "vacancy_change": "occupancy_rate",
        "employment_growth": "employment_per_capita",
        "school_enrollment_growth": "enrollment_per_capita",
        "ssp_projected_growth": "projected_population",
        "qsr_density_inv": "qsr_establishments",
    }

    # These indicators are inverted: higher raw value = lower score
    inverted = {"qsr_density_inv"}

    df = indicator_df
    available_cols = set(df.columns)

    for indicator_name, source_col in indicator_cols.items():
        if source_col not in available_cols:
            # Missing indicator — set normalized score to 0
            df = df.withColumn(f"_norm_{indicator_name}", lit(0.0))
            continue

        min_col = f"_min_{indicator_name}"
        max_col = f"_max_{indicator_name}"

        df = df.withColumn(min_col, spark_min(col(source_col)).over(Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)))
        df = df.withColumn(max_col, spark_max(col(source_col)).over(Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)))

        norm_col = f"_norm_{indicator_name}"
        range_expr = col(max_col) - col(min_col)
        normalized = when(
            range_expr == 0, lit(0.5)
        ).otherwise(
            (col(source_col) - col(min_col)) / range_expr
        )

        if indicator_name in inverted:
            normalized = lit(1.0) - normalized

        df = df.withColumn(norm_col, coalesce(normalized, lit(0.0)))
        df = df.drop(min_col, max_col)

    score_expr = sum(
        col(f"_norm_{name}") * lit(w[name])
        for name in w
    ) * 100

    df = df.withColumn("composite_score", score_expr)
    df = df.withColumn("score_tier", udf(assign_tier, StringType())(col("composite_score")))

    rank_window = Window.orderBy(col("composite_score").desc())
    df = df.withColumn("rank_national", row_number().over(rank_window))

    df = df.withColumn("component_scores", struct(
        *[col(f"_norm_{name}").alias(name) for name in w]
    ))

    for name in w:
        df = df.drop(f"_norm_{name}")

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
