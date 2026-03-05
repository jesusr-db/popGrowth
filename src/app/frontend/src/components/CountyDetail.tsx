import { useEffect, useState } from "react";
import {
  Radar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
} from "recharts";
import { api, CountyDetail as CountyDetailType, TrendPoint } from "../api/client";

interface Props {
  fips: string;
  onClose: () => void;
}

const INDICATOR_LABELS: Record<string, string> = {
  building_permits: "Building Permits",
  net_migration: "Net Migration",
  vacancy_change: "Vacancy Change",
  employment_growth: "Employment Growth",
  school_enrollment_growth: "School Enrollment",
  ssp_projected_growth: "SSP Projections",
  qsr_density_inv: "QSR White Space",
};

const TIER_BADGE_COLORS: Record<string, string> = {
  A: "#228B22", B: "#90EE90", C: "#FFD700", D: "#FF8C00", F: "#DC143C",
};

export function CountyDetail({ fips, onClose }: Props) {
  const [detail, setDetail] = useState<CountyDetailType | null>(null);
  const [trends, setTrends] = useState<TrendPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([api.getCounty(fips), api.getTrends(fips)]).then(
      ([d, t]) => {
        setDetail(d);
        setTrends(t);
        setLoading(false);
      }
    );
  }, [fips]);

  if (loading || !detail) return <div className="loading">Loading...</div>;

  const radarData = Object.entries(detail.component_scores).map(([key, value]) => ({
    indicator: INDICATOR_LABELS[key] || key,
    value: (value as number) * 100,
  }));

  const trendData = trends.map((t) => ({
    period: `${t.report_year} Q${t.report_quarter}`,
    ...t,
  }));

  return (
    <div className="county-detail">
      <div className="detail-header">
        <div>
          <h2>{detail.county_name}, {detail.state}</h2>
          <span
            className="tier-badge"
            style={{ backgroundColor: TIER_BADGE_COLORS[detail.score_tier] }}
          >
            Tier {detail.score_tier}
          </span>
          <span className="rank">Rank #{detail.rank_national}</span>
        </div>
        <button className="close-btn" onClick={onClose}>x</button>
      </div>

      <div className="score-display">
        <span className="big-score">{detail.composite_score.toFixed(1)}</span>
        <span className="score-label">/ 100</span>
      </div>

      <div className="metric-cards">
        <div className="card">
          <div className="card-value">{detail.population?.toLocaleString() ?? "N/A"}</div>
          <div className="card-label">Population</div>
        </div>
        <div className="card">
          <div className="card-value">
            ${detail.median_income?.toLocaleString() ?? "N/A"}
          </div>
          <div className="card-label">Median Income</div>
        </div>
        <div className="card">
          <div className="card-value">{detail.permits_per_1k_pop?.toFixed(1) ?? "N/A"}</div>
          <div className="card-label">Permits / 1K Pop</div>
        </div>
        <div className="card">
          <div className="card-value">{detail.net_migration_rate?.toFixed(2) ?? "N/A"}</div>
          <div className="card-label">Net Migration Rate</div>
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

      {trendData.length > 0 && (
        <>
          <h3>Historical Trends</h3>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={trendData}>
              <XAxis dataKey="period" tick={{ fontSize: 10 }} />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="permits_per_1k_pop" stroke="#228B22" name="Permits" dot={false} />
              <Line type="monotone" dataKey="net_migration_rate" stroke="#4A90D9" name="Migration" dot={false} />
              <Line type="monotone" dataKey="employment_growth_rate" stroke="#FF8C00" name="Employment" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </>
      )}
    </div>
  );
}
