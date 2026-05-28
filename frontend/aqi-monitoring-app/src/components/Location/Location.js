import React from 'react';
import { aqiColor, aqiCategory } from '../../utils/Aqihelpers';
import './style.css';

export default function Location({ location }) {
  if (!location) return null;

  const { name, coords, aqi, min, max, pm, hum, cluster } = location;
  const color = aqiColor(aqi);
  const category = aqiCategory(aqi);

  return (
    <div className="loc-card">
      {/* Top: name + AQI badge */}
      <div className="lc-top">
        <div>
          <div className="lc-name">{name}</div>
          <div className="lc-coords">{coords}</div>
        </div>
        <div
          className="aqi-badge"
          style={{ background: color + '22' }}
          aria-label={`Current AQI: ${aqi}, category: ${category}`}
        >
          <span className="aqi-num" style={{ color }}>{aqi}</span>
          <span className="aqi-cat" style={{ color }}>{category}</span>
        </div>
      </div>

      {/* Metrics grid */}
      <div className="lc-metrics">
        <div className="metric">
          <div className="metric-label">Min AQI</div>
          <div className="metric-val">{min}</div>
        </div>
        <div className="metric">
          <div className="metric-label">Max AQI</div>
          <div className="metric-val">{max}</div>
        </div>
        <div className="metric">
          <div className="metric-label">PM2.5</div>
          <div className="metric-val">{pm} µg</div>
        </div>
        <div className="metric">
          <div className="metric-label">Humidity</div>
          <div className="metric-val">{hum}%</div>
        </div>
      </div>

      {/* Tags */}
      <div className="lc-tags">
        <span className="tag tag-cluster">{cluster}</span>
        <span className="tag tag-active">● Monitoring Active</span>
      </div>
    </div>
  );
}