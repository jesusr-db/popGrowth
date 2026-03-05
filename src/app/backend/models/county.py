from pydantic import BaseModel


class ComponentScores(BaseModel):
    building_permits: float
    net_migration: float
    vacancy_change: float
    employment_growth: float
    school_enrollment_growth: float
    ssp_projected_growth: float
    qsr_density_inv: float


class CountySummary(BaseModel):
    fips: str
    county_name: str
    state: str
    composite_score: float
    score_tier: str
    rank_national: int
    population: int | None = None
    median_income: float | None = None


class CountyDetail(CountySummary):
    component_scores: ComponentScores
    permits_per_1k_pop: float | None = None
    net_migration_rate: float | None = None
    vacancy_rate_yoy_change: float | None = None
    employment_growth_rate: float | None = None
    enrollment_growth_rate: float | None = None


class ScoringWeight(BaseModel):
    indicator: str
    weight: float


class ScoringWeightsUpdate(BaseModel):
    weights: list[ScoringWeight]
