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
  ssp_projected_growth: "SSP Projections",
  qsr_density_inv: "QSR White Space",
};

const TIER_BADGE_COLORS: Record<string, string> = {
  A: "#228B22", B: "#90EE90", C: "#FFD700", D: "#FF8C00", F: "#DC143C",
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
  const radarData = Object.entries(INDICATOR_LABELS).map(([key, label]) => ({
    indicator: label,
    value: ((componentScores[key] as number) || 0) * 100,
  }));

  const fmt = (v: any, decimals = 1) =>
    v != null && v !== undefined ? Number(v).toFixed(decimals) : "N/A";

  const fmtInt = (v: any) =>
    v != null && v !== undefined ? Number(v).toLocaleString() : "N/A";

  return (
    <div className="county-detail">
      <div className="detail-header">
        <div>
          <h2>{detail.county_name || fips}, {detail.state || "??"}</h2>
          <span
            className="tier-badge"
            style={{ backgroundColor: TIER_BADGE_COLORS[detail.score_tier] || "#999" }}
          >
            Tier {detail.score_tier}
          </span>
          <span className="rank">Rank #{detail.rank_national}</span>
        </div>
        <button className="close-btn" onClick={onClose}>x</button>
      </div>

      <div className="score-display">
        <span className="big-score">{fmt(detail.composite_score)}</span>
        <span className="score-label">/ 100</span>
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
      </div>

      <h3>Component Scores</h3>
      <ResponsiveContainer width="100%" height={250}>
        <RadarChart data={radarData}>
          <PolarGrid />
          <PolarAngleAxis dataKey="indicator" tick={{ fontSize: 10 }} />
          <PolarRadiusAxis domain={[0, 100]} />
          <Radar dataKey="value" stroke="#4A90D9" fill="#4A90D9" fillOpacity={0.3} />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}
