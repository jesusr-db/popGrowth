import { useEffect, useState } from "react";
import {
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer,
} from "recharts";
import { api } from "../api/client";

interface Props {
  fips: string;
  onClose: () => void;
}

const INDICATOR_LABELS: Record<string, string> = {
  building_permits: "Building Permits",
  net_migration: "Net Migration",
  vacancy_change: "Occupancy",
  employment_growth: "Employment",
  school_enrollment_growth: "School Enrollment",
  ssp_projected_growth: "Pop. Projections",
};

const INDICATOR_TOOLTIPS: Record<string, string> = {
  building_permits: "New residential units permitted per 1,000 residents. High permit activity signals near-term rooftop growth — new customers arriving in 12-18 months.",
  net_migration: "Net move-ins minus move-outs per 1,000 residents (USPS data). Positive = people are arriving; negative = people are leaving.",
  vacancy_change: "Percentage of housing units occupied (1 − vacancy rate). Higher occupancy means stronger housing demand and more potential customers per square mile.",
  employment_growth: "Total employment divided by population. Higher ratios indicate a stronger local job market, more daytime traffic, and greater spending power.",
  school_enrollment_growth: "Public school enrollment relative to population. Growing enrollment signals family formation — a leading indicator of long-term residential demand.",
  ssp_projected_growth: "Blended county growth rate combining state-level SSP2 projections (50%), building permit momentum (25%), and net migration trends (25%).",
};

const TIER_BADGE_COLORS: Record<string, string> = {
  A: "#228B22", B: "#6BAF6B", C: "#E6A817", D: "#E07020", F: "#C41E3A",
};

const TIER_LABELS: Record<string, string> = {
  A: "Top 10%", B: "Top 30%", C: "Top 60%", D: "Top 85%", F: "Bottom 15%",
};

export function CountyDetail({ fips, onClose }: Props) {
  const [detail, setDetail] = useState<any | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    api.getCounty(fips)
      .then((d) => {
        setDetail(d);
        setLoading(false);
      })
      .catch((e) => {
        setError(e.message);
        setLoading(false);
      });
  }, [fips]);

  if (loading) return <div className="loading">Loading...</div>;
  if (error) return <div className="loading">Error: {error}</div>;
  if (!detail) return null;

  const componentScores = detail.component_scores || {};
  const radarData = Object.entries(INDICATOR_LABELS).map(([key, label]) => {
    const raw = componentScores[key];
    const hasData = raw != null && raw !== undefined;
    return {
      key,
      indicator: label,
      value: hasData ? Math.round(raw * 100) : 0,
      hasData,
      tooltip: INDICATOR_TOOLTIPS[key] || "",
    };
  });

  const fmt = (v: any, decimals = 1) =>
    v != null && v !== undefined ? Number(v).toFixed(decimals) : "N/A";

  const fmtInt = (v: any) =>
    v != null && v !== undefined ? Number(v).toLocaleString() : "N/A";

  const fmtPct = (v: any) =>
    v != null && v !== undefined ? `${(Number(v) * 100).toFixed(1)}%` : "N/A";

  const tier = detail.score_tier || "F";
  const tierColor = TIER_BADGE_COLORS[tier] || "#999";

  return (
    <div className="county-detail">
      <div className="detail-header">
        <div>
          <h2>{detail.county_name || fips}, {detail.state || "??"}</h2>
          <span className="tier-badge" style={{ backgroundColor: tierColor }}>
            Tier {tier}
          </span>
          <span className="tier-pctile">{TIER_LABELS[tier]}</span>
          <span className="rank">Rank #{detail.rank_national} of 3,209</span>
        </div>
        <button className="close-btn" onClick={onClose}>×</button>
      </div>

      <div className="score-display">
        <div className="score-ring" style={{ borderColor: tierColor }}>
          <span className="big-score">{fmt(detail.composite_score, 0)}</span>
        </div>
        <div className="score-sublabel">Growth Score</div>
      </div>

      <div className="metric-cards">
        <div className="card">
          <div className="card-value">{fmtInt(detail.population)}</div>
          <div className="card-label">Population</div>
        </div>
        <div className="card">
          <div className="card-value">
            {detail.median_income != null ? `$${fmtInt(detail.median_income)}` : "N/A"}
          </div>
          <div className="card-label">Median Income</div>
        </div>
        <div className="card">
          <div className="card-value">{fmt(detail.permits_per_1k_pop)}</div>
          <div className="card-label">Permits / 1K Pop</div>
        </div>
        <div className="card">
          <div className="card-value">
            {detail.avg_weekly_wage != null ? `$${fmt(detail.avg_weekly_wage, 0)}` : "N/A"}
          </div>
          <div className="card-label">Avg Weekly Wage</div>
        </div>
        <div className="card">
          <div className="card-value">
            {detail.net_migration_rate != null ? fmtPct(detail.net_migration_rate / 100) : "N/A"}
          </div>
          <div className="card-label">Net Migration Rate</div>
        </div>
        <div className="card">
          <div className="card-value">
            {detail.occupancy_rate != null ? fmtPct(detail.occupancy_rate) : "N/A"}
          </div>
          <div className="card-label">Occupancy Rate</div>
        </div>
      </div>

      {(detail.ssp_projected_pop || detail.ssp_growth_rate != null) && (
        <div className="projection-section">
          <h3>Population Projections (SSP2)</h3>
          <div className="projection-cards">
            <div className="projection-card">
              <div className="projection-value">{fmtInt(detail.population)}</div>
              <div className="projection-label">Current Pop.</div>
            </div>
            <div className="projection-arrow">→</div>
            <div className="projection-card">
              <div className="projection-value">{fmtInt(detail.ssp_projected_pop)}</div>
              <div className="projection-label">
                Projected {detail.ssp_projection_year || ""}
              </div>
            </div>
          </div>
          {detail.ssp_growth_rate != null && (
            <div className={`projection-growth ${detail.ssp_growth_rate >= 0 ? "positive" : "negative"}`}>
              {detail.ssp_growth_rate >= 0 ? "▲" : "▼"}{" "}
              {fmtPct(Math.abs(detail.ssp_growth_rate))} projected growth
            </div>
          )}
          <div className="projection-note">
            Based on IIASA SSP2 (middle-of-the-road) scenario at state level
          </div>
        </div>
      )}

      <h3>Component Scores</h3>
      <ResponsiveContainer width="100%" height={260}>
        <RadarChart data={radarData}>
          <PolarGrid />
          <PolarAngleAxis dataKey="indicator" tick={{ fontSize: 10 }} />
          <PolarRadiusAxis domain={[0, 100]} tick={{ fontSize: 9 }} />
          <Radar dataKey="value" stroke={tierColor} fill={tierColor} fillOpacity={0.25} />
        </RadarChart>
      </ResponsiveContainer>

      <div className="component-bars">
        {radarData.map((d) => (
          <div key={d.indicator} className="bar-row" title={d.tooltip}>
            <span className="bar-label">
              {d.indicator}
              <span className="info-icon" title={d.tooltip}>ⓘ</span>
            </span>
            <div className="bar-track">
              {d.hasData ? (
                <div
                  className="bar-fill"
                  style={{ width: `${d.value}%`, backgroundColor: tierColor }}
                />
              ) : (
                <div className="bar-fill bar-fill-na" style={{ width: "100%" }} />
              )}
            </div>
            <span className="bar-value">{d.hasData ? d.value : "N/A"}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
