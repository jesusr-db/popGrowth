import { useState, useEffect } from "react";
import { NationalMap } from "./components/NationalMap";
import { CountyDetail } from "./components/CountyDetail";
import { WeightTuner } from "./components/WeightTuner";
import { AnalysisView } from "./components/AnalysisView";
import { api } from "./api/client";
import "./App.css";

type Tab = "map" | "analysis";

export default function App() {
  const [activeTab, setActiveTab] = useState<Tab>("map");
  const [selectedFips, setSelectedFips] = useState<string | null>(null);
  const [showWeightTuner, setShowWeightTuner] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [geojson, setGeojson] = useState<any | null>(null);

  useEffect(() => {
    api.getGeoJson().then(setGeojson);
  }, [refreshKey]);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Store Siting — Growth Score Explorer</h1>
        <nav className="tab-nav">
          <button
            className={`tab-btn ${activeTab === "map" ? "active" : ""}`}
            onClick={() => setActiveTab("map")}
          >
            Map
          </button>
          <button
            className={`tab-btn ${activeTab === "analysis" ? "active" : ""}`}
            onClick={() => setActiveTab("analysis")}
          >
            Analysis
          </button>
        </nav>
        <button onClick={() => setShowWeightTuner(!showWeightTuner)}>
          {showWeightTuner ? "Hide Weights" : "Adjust Weights"}
        </button>
      </header>

      <div className="app-body">
        {activeTab === "map" && (
          <>
            <div className="map-container">
              <NationalMap
                geojson={geojson}
                onSelectCounty={setSelectedFips}
              />
            </div>
            {selectedFips && (
              <div className="detail-panel">
                <CountyDetail
                  fips={selectedFips}
                  onClose={() => setSelectedFips(null)}
                />
              </div>
            )}
          </>
        )}

        {activeTab === "analysis" && (
          <AnalysisView
            geojson={geojson}
            onSelectCounty={setSelectedFips}
            selectedFips={selectedFips}
            onCloseDetail={() => setSelectedFips(null)}
          />
        )}

        {showWeightTuner && (
          <div className="weight-panel">
            <WeightTuner onRecalculate={() => setRefreshKey((k) => k + 1)} />
          </div>
        )}
      </div>
    </div>
  );
}
