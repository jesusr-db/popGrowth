import { useState, useMemo } from "react";

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

const TIER_DOT: Record<string, string> = {
  A: "#15803d", B: "#4aaf64", C: "#e6a817", D: "#e07020", F: "#c41e3a",
};

type SortKey = "rank" | "name" | "state" | "score" | "tier" | "population" | "income";
type SortDir = "asc" | "desc";

interface Props {
  features: any[];
  matchingFips: Set<string>;
  onSelect: (fips: string) => void;
}

export function ResultsList({ features, matchingFips, onSelect }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("score");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const rows = useMemo(() => {
    const matched = features
      .filter(f => matchingFips.has(f.properties?.GEOID))
      .map(f => f.properties);

    matched.sort((a, b) => {
      let va: any, vb: any;
      switch (sortKey) {
        case "rank": va = a.rank_national; vb = b.rank_national; break;
        case "name": va = a.NAME || ""; vb = b.NAME || ""; break;
        case "state": va = STATE_FIPS[a.STATE] || ""; vb = STATE_FIPS[b.STATE] || ""; break;
        case "score": va = a.composite_score || 0; vb = b.composite_score || 0; break;
        case "tier": va = a.score_tier || "Z"; vb = b.score_tier || "Z"; break;
        case "population": va = a.population || 0; vb = b.population || 0; break;
        case "income": va = a.median_income || 0; vb = b.median_income || 0; break;
      }
      if (typeof va === "string") {
        const cmp = va.localeCompare(vb);
        return sortDir === "asc" ? cmp : -cmp;
      }
      return sortDir === "asc" ? va - vb : vb - va;
    });

    return matched;
  }, [features, matchingFips, sortKey, sortDir]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir(key === "name" || key === "state" ? "asc" : "desc");
    }
  };

  const arrow = (key: SortKey) =>
    sortKey === key ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  return (
    <div className="results-list">
      <table className="results-table">
        <thead>
          <tr>
            <th onClick={() => toggleSort("rank")}>#{ arrow("rank")}</th>
            <th onClick={() => toggleSort("name")}>County{arrow("name")}</th>
            <th onClick={() => toggleSort("state")}>ST{arrow("state")}</th>
            <th onClick={() => toggleSort("score")}>Score{arrow("score")}</th>
            <th onClick={() => toggleSort("tier")}>Tier{arrow("tier")}</th>
            <th onClick={() => toggleSort("population")}>Pop.{arrow("population")}</th>
            <th onClick={() => toggleSort("income")}>Income{arrow("income")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((p) => (
            <tr key={p.GEOID} onClick={() => onSelect(p.GEOID)} className="results-row">
              <td className="col-rank">{p.rank_national}</td>
              <td className="col-name">{p.NAME || p.GEOID}</td>
              <td className="col-state">{STATE_FIPS[p.STATE] || p.STATE}</td>
              <td className="col-score">{p.composite_score?.toFixed(1)}</td>
              <td className="col-tier">
                <span className="tier-dot" style={{ background: TIER_DOT[p.score_tier] || "#999" }} />
                {p.score_tier}
              </td>
              <td className="col-pop">{p.population?.toLocaleString()}</td>
              <td className="col-income">{p.median_income ? `$${Math.round(p.median_income).toLocaleString()}` : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
