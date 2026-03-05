import pytest
from pyspark.sql import SparkSession
from src.silver.vacancy import transform_vacancy
from src.silver.employment import transform_employment
from src.silver.school_enrollment import transform_school_enrollment
from src.silver.acs_demographics import transform_acs_demographics
from src.silver.ssp_projections import transform_ssp_projections
from src.silver.national_projections import transform_national_projections


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_transform_vacancy_computes_rate(spark):
    data = [
        ("12086", 2025, 1, 100000, 5000),
        ("12086", 2025, 2, 100000, 4500),
    ]
    df = spark.createDataFrame(data, ["fips", "report_year", "report_quarter", "total_addresses", "vacant_addresses"])
    result = transform_vacancy(df)
    rows = result.collect()
    assert len(rows) == 2
    assert "vacancy_rate" in result.columns
    assert "vacancy_rate_yoy_change" in result.columns


def test_transform_employment_computes_growth(spark):
    data = [
        ("12086", 2025, 1, 50000, 500000000, 1000),
        ("12086", 2025, 2, 52000, 520000000, 1020),
    ]
    df = spark.createDataFrame(data, ["fips", "report_year", "report_quarter", "total_employment", "total_wages", "establishments"])
    result = transform_employment(df)
    assert "employment_growth_rate" in result.columns
    assert "avg_weekly_wage" in result.columns


def test_transform_school_enrollment_aggregates(spark):
    data = [
        ("12086", 2024, "D001", 5000),
        ("12086", 2024, "D002", 3000),
        ("12086", 2025, "D001", 5200),
        ("12086", 2025, "D002", 3100),
    ]
    df = spark.createDataFrame(data, ["fips", "report_year", "district_id", "total_enrollment"])
    result = transform_school_enrollment(df)
    rows = result.collect()
    assert len(rows) == 2


def test_transform_acs_demographics(spark):
    data = [("12086", 2025, 2800000, 55000.0, 38.5, 950000)]
    df = spark.createDataFrame(data, ["fips", "report_year", "population", "median_income", "median_age", "households"])
    result = transform_acs_demographics(df)
    assert result.count() == 1
    assert "population" in result.columns


def test_transform_ssp_projections(spark):
    data = [
        ("12086", 2030, "SSP2", 3000000),
        ("12086", 2040, "SSP2", 3500000),
    ]
    df = spark.createDataFrame(data, ["fips", "projection_year", "scenario", "projected_population"])
    result = transform_ssp_projections(df)
    assert "projected_growth_rate" in result.columns


def test_transform_national_projections(spark):
    data = [("12", 2030, 25000000)]
    df = spark.createDataFrame(data, ["state_fips", "projection_year", "projected_population"])
    result = transform_national_projections(df)
    assert result.count() == 1
