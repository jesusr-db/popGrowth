import { render, screen } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { WeightTuner } from "../WeightTuner";

// Mock the API
vi.mock("../../api/client", () => ({
  api: {
    getWeights: vi.fn().mockResolvedValue([
      { indicator: "building_permits", weight: 0.25 },
      { indicator: "net_migration", weight: 0.20 },
      { indicator: "vacancy_change", weight: 0.15 },
      { indicator: "employment_growth", weight: 0.15 },
      { indicator: "school_enrollment_growth", weight: 0.10 },
      { indicator: "ssp_projected_growth", weight: 0.10 },
      { indicator: "qsr_density_inv", weight: 0.05 },
    ]),
    updateWeights: vi.fn().mockResolvedValue({ ok: true }),
  },
}));

describe("WeightTuner", () => {
  it("renders heading", async () => {
    render(<WeightTuner onRecalculate={vi.fn()} />);
    expect(screen.getByText("Scoring Weights")).toBeInTheDocument();
  });

  it("shows recalculate button", async () => {
    render(<WeightTuner onRecalculate={vi.fn()} />);
    expect(screen.getByText("Recalculate Scores")).toBeInTheDocument();
  });
});
