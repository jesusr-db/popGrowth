import { useEffect, useState } from "react";
import { api, ScoringWeight } from "../api/client";

const INDICATOR_LABELS: Record<string, string> = {
  building_permits: "Building Permits",
  net_migration: "Net Migration",
  vacancy_change: "Vacancy Change",
  employment_growth: "Employment Growth",
  school_enrollment_growth: "School Enrollment",
  ssp_projected_growth: "SSP Projections",
  qsr_density_inv: "QSR White Space",
};

interface Props {
  onRecalculate: () => void;
}

export function WeightTuner({ onRecalculate }: Props) {
  const [weights, setWeights] = useState<ScoringWeight[]>([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.getWeights().then(setWeights);
  }, []);

  const total = weights.reduce((sum, w) => sum + w.weight, 0);
  const isValid = Math.abs(total - 1.0) < 0.01;

  const handleChange = (indicator: string, value: number) => {
    setWeights((prev) =>
      prev.map((w) => (w.indicator === indicator ? { ...w, weight: value } : w))
    );
  };

  const handleRecalculate = async () => {
    if (!isValid) return;
    setSaving(true);
    await api.updateWeights(weights);
    setSaving(false);
    onRecalculate();
  };

  return (
    <div className="weight-tuner">
      <h3>Scoring Weights</h3>
      <p className={`total ${isValid ? "valid" : "invalid"}`}>
        Total: {(total * 100).toFixed(0)}% {isValid ? "" : "(must equal 100%)"}
      </p>

      {weights.map((w) => (
        <div key={w.indicator} className="weight-slider">
          <label>
            {INDICATOR_LABELS[w.indicator] || w.indicator}:
            {(w.weight * 100).toFixed(0)}%
          </label>
          <input
            type="range"
            min={0}
            max={0.5}
            step={0.01}
            value={w.weight}
            onChange={(e) => handleChange(w.indicator, Number(e.target.value))}
          />
        </div>
      ))}

      <button
        className="recalculate-btn"
        onClick={handleRecalculate}
        disabled={!isValid || saving}
      >
        {saving ? "Saving..." : "Recalculate Scores"}
      </button>
    </div>
  );
}
