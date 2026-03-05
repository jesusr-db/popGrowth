from pydantic import BaseModel


class ComponentScores(BaseModel):
    building_permits: float = 0
    net_migration: float = 0
    vacancy_change: float = 0
    employment_growth: float = 0
    school_enrollment_growth: float = 0
    ssp_projected_growth: float = 0
    qsr_density_inv: float = 0


class CountySummary(BaseModel):
    fips: str
    county_name: str | None = None
    state: str | None = None
    composite_score: float = 0
    score_tier: str = "F"
    rank_national: int | None = None
    population: int | None = None
    median_income: float | None = None


class CountyDetail(CountySummary):
    component_scores: ComponentScores | None = None
    permits_per_1k_pop: float | None = None
    net_migration_rate: float | None = None
    occupancy_rate: float | None = None
    employment_per_capita: float | None = None
    enrollment_per_capita: float | None = None
    avg_weekly_wage: float | None = None
    vacancy_rate: float | None = None


class ScoringWeight(BaseModel):
    indicator: str
    weight: float


class ScoringWeightsUpdate(BaseModel):
    weights: list[ScoringWeight]
