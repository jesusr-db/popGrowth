export interface Filters {
  score: [number, number];
  tiers: string[];
  states: string[];
  population: [number, number];
  median_income: [number, number];
  permits_per_1k_pop: [number, number];
  net_migration_rate: [number, number];
  occupancy_rate: [number, number];
  employment_per_capita: [number, number];
  ssp_growth_rate: [number, number];
}

export interface DataBounds {
  population: [number, number];
  median_income: [number, number];
  permits_per_1k_pop: [number, number];
  net_migration_rate: [number, number];
  occupancy_rate: [number, number];
  employment_per_capita: [number, number];
  ssp_growth_rate: [number, number];
}

export function computeBounds(features: any[]): DataBounds {
  const vals = (key: string) => features
    .map(f => f.properties?.[key])
    .filter(v => v != null && isFinite(v));

  const range = (arr: number[]): [number, number] =>
    arr.length ? [Math.min(...arr), Math.max(...arr)] : [0, 0];

  return {
    population: range(vals("population")),
    median_income: range(vals("median_income")),
    permits_per_1k_pop: range(vals("permits_per_1k_pop")),
    net_migration_rate: range(vals("net_migration_rate")),
    occupancy_rate: range(vals("occupancy_rate")),
    employment_per_capita: range(vals("employment_per_capita")),
    ssp_growth_rate: range(vals("ssp_growth_rate")),
  };
}

export function defaultFilters(bounds: DataBounds): Filters {
  return {
    score: [0, 100],
    tiers: ["A", "B", "C", "D", "F"],
    states: [],
    population: bounds.population,
    median_income: bounds.median_income,
    permits_per_1k_pop: bounds.permits_per_1k_pop,
    net_migration_rate: bounds.net_migration_rate,
    occupancy_rate: bounds.occupancy_rate,
    employment_per_capita: bounds.employment_per_capita,
    ssp_growth_rate: bounds.ssp_growth_rate,
  };
}

function inRange(val: any, range: [number, number]): boolean {
  if (val == null || !isFinite(val)) return true; // don't exclude missing data
  return val >= range[0] && val <= range[1];
}

export function filterFeatures(features: any[], filters: Filters): Set<string> {
  const matching = new Set<string>();
  for (const f of features) {
    const p = f.properties || {};
    const fips = p.GEOID;
    if (!fips) continue;

    if (!inRange(p.composite_score, filters.score)) continue;
    if (filters.tiers.length < 5 && !filters.tiers.includes(p.score_tier)) continue;
    if (filters.states.length > 0 && !filters.states.includes(p.STATE)) continue;
    if (!inRange(p.population, filters.population)) continue;
    if (!inRange(p.median_income, filters.median_income)) continue;
    if (!inRange(p.permits_per_1k_pop, filters.permits_per_1k_pop)) continue;
    if (!inRange(p.net_migration_rate, filters.net_migration_rate)) continue;
    if (!inRange(p.occupancy_rate, filters.occupancy_rate)) continue;
    if (!inRange(p.employment_per_capita, filters.employment_per_capita)) continue;
    if (!inRange(p.ssp_growth_rate, filters.ssp_growth_rate)) continue;

    matching.add(fips);
  }
  return matching;
}
