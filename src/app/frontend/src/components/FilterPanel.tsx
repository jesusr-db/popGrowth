import { useMemo } from "react";
import { Filters, DataBounds, defaultFilters } from "./analysisFilters";

const STATE_FIPS: Record<string, string> = {
  "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
  "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
  "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
  "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
  "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
  "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
  "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
  "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
  "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
  "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
  "56": "WY",
};

interface Props {
  filters: Filters;
  bounds: DataBounds;
  matchCount: number;
  totalCount: number;
  states: string[];
  onChange: (f: Filters) => void;
}

function RangeSlider({ label, value, min, max, step, fmt, onChange }: {
  label: string;
  value: [number, number];
  min: number;
  max: number;
  step?: number;
  fmt?: (v: number) => string;
  onChange: (v: [number, number]) => void;
}) {
  const format = fmt || ((v: number) => v.toLocaleString());
  const s = step || (max - min > 100 ? Math.round((max - min) / 100) : 0.01);
  return (
    <div className="filter-group">
      <label className="filter-label">
        {label}
        <span className="filter-range-text">{format(value[0])} — {format(value[1])}</span>
      </label>
      <div className="dual-range">
        <input type="range" min={min} max={max} step={s} value={value[0]}
          onChange={e => onChange([Math.min(Number(e.target.value), value[1]), value[1]])} />
        <input type="range" min={min} max={max} step={s} value={value[1]}
          onChange={e => onChange([value[0], Math.max(Number(e.target.value), value[0])])} />
      </div>
    </div>
  );
}

export function FilterPanel({ filters, bounds, matchCount, totalCount, states, onChange }: Props) {
  const stateEntries = useMemo(() =>
    states.map(s => ({ fips: s, abbr: STATE_FIPS[s] || s }))
      .sort((a, b) => a.abbr.localeCompare(b.abbr)),
    [states]
  );

  const setFilter = <K extends keyof Filters>(key: K, val: Filters[K]) =>
    onChange({ ...filters, [key]: val });

  const reset = () => onChange(defaultFilters(bounds));

  const fmtDollar = (v: number) => `$${Math.round(v).toLocaleString()}`;
  const fmtPct = (v: number) => `${(v * 100).toFixed(1)}%`;
  const fmtDec = (v: number) => v.toFixed(2);

  return (
    <div className="filter-panel">
      <div className="filter-header">
        <span className="match-count">
          <strong>{matchCount.toLocaleString()}</strong> of {totalCount.toLocaleString()} counties
        </span>
        <button className="reset-btn" onClick={reset}>Reset</button>
      </div>

      <RangeSlider label="Composite Score" value={filters.score}
        min={0} max={100} step={1} fmt={v => v.toFixed(0)} onChange={v => setFilter("score", v)} />

      <div className="filter-group">
        <label className="filter-label">Tier</label>
        <div className="tier-checkboxes">
          {["A", "B", "C", "D", "F"].map(t => (
            <label key={t} className="tier-check">
              <input type="checkbox" checked={filters.tiers.includes(t)}
                onChange={e => {
                  const next = e.target.checked
                    ? [...filters.tiers, t]
                    : filters.tiers.filter(x => x !== t);
                  setFilter("tiers", next);
                }} />
              {t}
            </label>
          ))}
        </div>
      </div>

      <div className="filter-group">
        <label className="filter-label">State</label>
        <select multiple className="state-multi-select" value={filters.states}
          onChange={e => setFilter("states",
            Array.from(e.target.selectedOptions, o => o.value)
          )}>
          {stateEntries.map(s => (
            <option key={s.fips} value={s.fips}>{s.abbr}</option>
          ))}
        </select>
        {filters.states.length > 0 && (
          <button className="clear-states" onClick={() => setFilter("states", [])}>
            Clear ({filters.states.length})
          </button>
        )}
      </div>

      <RangeSlider label="Population" value={filters.population}
        min={bounds.population[0]} max={bounds.population[1]}
        fmt={v => v.toLocaleString()} onChange={v => setFilter("population", v)} />

      <RangeSlider label="Median Income" value={filters.median_income}
        min={bounds.median_income[0]} max={bounds.median_income[1]}
        fmt={fmtDollar} onChange={v => setFilter("median_income", v)} />

      <RangeSlider label="Permits / 1K Pop" value={filters.permits_per_1k_pop}
        min={bounds.permits_per_1k_pop[0]} max={bounds.permits_per_1k_pop[1]}
        fmt={fmtDec} onChange={v => setFilter("permits_per_1k_pop", v)} />

      <RangeSlider label="Net Migration Rate" value={filters.net_migration_rate}
        min={bounds.net_migration_rate[0]} max={bounds.net_migration_rate[1]}
        fmt={fmtDec} onChange={v => setFilter("net_migration_rate", v)} />

      <RangeSlider label="Occupancy Rate" value={filters.occupancy_rate}
        min={bounds.occupancy_rate[0]} max={bounds.occupancy_rate[1]}
        fmt={fmtPct} onChange={v => setFilter("occupancy_rate", v)} />

      <RangeSlider label="Employment / Capita" value={filters.employment_per_capita}
        min={bounds.employment_per_capita[0]} max={bounds.employment_per_capita[1]}
        fmt={fmtDec} onChange={v => setFilter("employment_per_capita", v)} />

      <RangeSlider label="SSP Growth Rate" value={filters.ssp_growth_rate}
        min={bounds.ssp_growth_rate[0]} max={bounds.ssp_growth_rate[1]}
        fmt={fmtPct} onChange={v => setFilter("ssp_growth_rate", v)} />
    </div>
  );
}
