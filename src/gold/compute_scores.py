"""Gold scoring job — join all Silver tables, normalize, score, write Gold."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, struct, lit, udf, row_number, coalesce,
    count as spark_count,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

from src.common.config import CATALOG, SILVER_SCHEMA, GOLD_SCHEMA, DEFAULT_WEIGHTS, MIN_SOURCES_FOR_SCORE
from src.gold.scoring import compute_composite_score, assign_tier


def build_indicator_table(spark: SparkSession, catalog: str = CATALOG) -> DataFrame:
    """Join all Silver tables into a single indicator table per county per quarter."""
    silver = f"{catalog}.{SILVER_SCHEMA}"

    base = (
        spark.table(f"{silver}.silver_acs_demographics")
        .select("fips", "report_year", col("population"), col("median_income"))
    )

    permits = (
        spark.table(f"{silver}.silver_building_permits")
        .select("fips", "report_year", "report_quarter",
                col("total_units_permitted"),
                col("single_family_units").alias("bp_single"),
                col("multi_family_units").alias("bp_multi"))
    )

    migration = (
        spark.table(f"{silver}.silver_migration")
        .select("fips", "report_year", "report_quarter",
                "net_migration", "net_migration_rate")
    )

    vacancy = (
        spark.table(f"{silver}.silver_vacancy")
        .select("fips", "report_year", "report_quarter",
                "vacancy_rate", "vacancy_rate_yoy_change")
    )

    employment = (
        spark.table(f"{silver}.silver_employment")
        .select("fips", "report_year", "report_quarter",
                "employment_growth_rate", "avg_weekly_wage")
    )

    school = (
        spark.table(f"{silver}.silver_school_enrollment")
        .select("fips", "report_year", "enrollment_growth_rate")
    )

    business = (
        spark.table(f"{silver}.silver_business_patterns")
        .select("fips", "report_year", "qsr_establishments", "retail_density")
    )

    ssp = (
        spark.table(f"{silver}.silver_ssp_projections")
        .filter(col("scenario") == "SSP2")
        .select("fips", col("projection_year").alias("report_year"),
                col("projected_population"))
    )

    join_keys_yr = ["fips", "report_year"]
    join_keys_qtr = ["fips", "report_year", "report_quarter"]

    latest_q = (
        permits.join(migration, on=join_keys_qtr, how="outer")
               .join(vacancy, on=join_keys_qtr, how="outer")
               .join(employment, on=join_keys_qtr, how="outer")
    )

    combined = (
        latest_q
        .join(base, on=join_keys_yr[:2], how="outer")
        .join(school, on=join_keys_yr[:2], how="left")
        .join(business, on=join_keys_yr[:2], how="left")
        .join(ssp, on=join_keys_yr[:2], how="left")
    )

    combined = combined.withColumn(
        "permits_per_1k_pop",
        coalesce(col("total_units_permitted"), lit(0)) / col("population") * 1000
    )

    return combined


def score_counties(indicator_df: DataFrame, weights: dict | None = None) -> DataFrame:
    """Normalize indicators and compute composite scores."""
    w = weights or DEFAULT_WEIGHTS

    from pyspark.sql.functions import min as spark_min, max as spark_max

    indicator_cols = {
        "building_permits": "permits_per_1k_pop",
        "net_migration": "net_migration_rate",
        "vacancy_change": "vacancy_rate_yoy_change",
        "employment_growth": "employment_growth_rate",
        "school_enrollment_growth": "enrollment_growth_rate",
        "ssp_projected_growth": "projected_population",
        "qsr_density_inv": "qsr_establishments",
    }

    inverted = {"vacancy_change", "qsr_density_inv"}

    df = indicator_df
    for indicator_name, source_col in indicator_cols.items():
        min_col = f"_min_{indicator_name}"
        max_col = f"_max_{indicator_name}"

        df = df.withColumn(min_col, spark_min(col(source_col)).over(Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)))
        df = df.withColumn(max_col, spark_max(col(source_col)).over(Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)))

        norm_col = f"_norm_{indicator_name}"
        range_expr = col(max_col) - col(min_col)
        normalized = (col(source_col) - col(min_col)) / range_expr

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

    scored_df.select(
        "fips", "report_year", "report_quarter",
        "population", "median_income",
        "composite_score", "score_tier", "rank_national",
        "component_scores",
    ).write.mode("overwrite").saveAsTable(f"{catalog}.{GOLD_SCHEMA}.gold_county_growth_score")

    indicator_df.write.mode("overwrite").saveAsTable(f"{catalog}.{GOLD_SCHEMA}.gold_county_details")

    from pyspark.sql import Row
    config_rows = [Row(indicator=k, weight=v) for k, v in DEFAULT_WEIGHTS.items()]
    spark.createDataFrame(config_rows).write.mode("overwrite").saveAsTable(
        f"{catalog}.{GOLD_SCHEMA}.gold_scoring_config"
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold-scoring").getOrCreate()
    run_gold_scoring(spark)
