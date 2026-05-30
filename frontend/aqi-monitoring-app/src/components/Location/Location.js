import React, { useState,useEffect } from 'react';
import { aqiColor, aqiCategory } from '../../utils/Aqihelpers';
import './style.css';

export default function Location({ location }) {
  const [hourData,setHourData]=useState([]);
  useEffect(() => {
      if (!location) return;
      async function fetchAndRender() {
        try {
          const response = await fetch('http://127.0.0.1:8000/location-data', {
            method: 'POST',
            headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
            body: JSON.stringify({ district: location.district, city: location.city }),
          });
          if (response.ok) {
           const  result = await response.json();
           setHourData(result)
          }
        } catch (err) {
          console.error('Failed to fetch hour data:', err);
        }
      }
      fetchAndRender();
    }, [location]);
  if (!location) return null;
  const color =location.color;
  const category =location.label;
 
  
  return (
    <div className="loc-card">
      {/* Top: name + AQI badge */}
      <div className="lc-top">
        <div>
          <div className="lc-name">{location.district+' '+location.city}</div>
          <div className="lc-coords">{location.lat+'-'+location.lon}</div>
        </div>
        <div
          className="aqi-badge"
          style={{ background: color + '22' }}
          aria-label={`Current AQI: ${location.aqi_final}, category: ${location.label}`}
        >
          <span className="aqi-num" style={{ color }}>{location.aqi_final}</span>
          <span className="aqi-cat" style={{ color }}>{location.label}</span>
        </div>
      </div>

      {/* Metrics grid */}
      <div className="lc-metrics">
       {hourData &&<div className="metric">
          <div className="metric-label">Min AQI</div>
          <div className="metric-val">{hourData.map((it)=>(it.value)).sort((a,b)=>(a-b))[0]}</div>
        </div>}
        <div className="metric">
          <div className="metric-label">Max AQI</div>
          <div className="metric-val">{hourData.map((it)=>(it.value)).sort((a,b)=>(b-a))[0]}</div>
        </div>
        <div className="metric">
          <div className="metric-label">PM2.5</div>
          <div className="metric-val">{location.pm_25} µg</div>
        </div>
        {/* <div className="metric">
          <div className="metric-label">Humidity</div>
          <div className="metric-val">{hum}%</div>
        </div> */}
      </div>

      {/* Tags */}
      <div className="lc-tags">
        <span className="tag tag-cluster">{0}</span>
        <span className="tag tag-active" style={{backgroundColor:color,color:'black'}}>{location.advice}</span>
      </div>
    </div>
  );
}