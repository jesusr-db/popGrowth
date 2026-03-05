const BASE_URL = "/api";

export interface CountySummary {
  fips: string;
  county_name: string;
  state: string;
  composite_score: number;
  score_tier: string;
  rank_national: number;
  population?: number;
  median_income?: number;
}

export interface ComponentScores {
  building_permits: number;
  net_migration: number;
  vacancy_change: number;
  employment_growth: number;
  school_enrollment_growth: number;
  ssp_projected_growth: number;
}

export interface CountyDetail extends CountySummary {
  component_scores: ComponentScores;
  permits_per_1k_pop?: number;
  net_migration_rate?: number;
  occupancy_rate?: number;
  employment_per_capita?: number;
  enrollment_per_capita?: number;
  avg_weekly_wage?: number;
  vacancy_rate?: number;
  ssp_projected_pop?: number;
  ssp_projection_year?: number;
  ssp_growth_rate?: number;
}

export interface ScoringWeight {
  indicator: string;
  weight: number;
}

export interface TrendPoint {
  report_year: number;
  report_quarter: number;
  permits_per_1k_pop?: number;
  net_migration_rate?: number;
  vacancy_rate_yoy_change?: number;
  employment_growth_rate?: number;
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export const api = {
  getCounties: (state?: string) =>
    fetchJson<CountySummary[]>(`/counties${state ? `?state=${state}` : ""}`),

  getTopCounties: (n = 25, state?: string) =>
    fetchJson<CountySummary[]>(`/counties/top?n=${n}${state ? `&state=${state}` : ""}`),

  getCounty: (fips: string) =>
    fetchJson<CountyDetail>(`/counties/${fips}`),

  getGeoJson: () => fetchJson<any>("/geojson"),

  getTrends: (fips: string) =>
    fetchJson<TrendPoint[]>(`/trends/${fips}`),

  getWeights: () =>
    fetchJson<ScoringWeight[]>("/scores/weights"),

  updateWeights: (weights: ScoringWeight[]) =>
    fetch(`${BASE_URL}/scores/weights`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ weights }),
    }),
};
