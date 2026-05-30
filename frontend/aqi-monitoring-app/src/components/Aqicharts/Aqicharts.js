import React, { useEffect, useRef, useState } from 'react';
import { Chart as ChartJS, registerables } from 'chart.js';
import { aqiColor, gen24hData } from '../../utils/Aqihelpers';
import './style.css';

ChartJS.register(...registerables);

const CHART_SCALE_STYLE = {
  x: { ticks: { font: { size: 9 }, color: '#64748b' }, grid: { color: '#1e293b55' } },
  y: { ticks: { font: { size: 9 }, color: '#64748b' }, grid: { color: '#1e293b55' } },
};

/**
 * AQICharts
 * Props:
 *   location  {object}   — currently selected location object
 *   locations {Array}    — all locations (for district comparison chart)
 */
export default function AQICharts({ location, locations }) {
  const trendRef  = useRef(null);
  const weekRef   = useRef(null);
  const pollRef   = useRef(null);
  const compRef   = useRef(null);

  const trendChart = useRef(null);
  const weekChart  = useRef(null);
  const pollChart  = useRef(null);
  const compChart  = useRef(null);
  const [hourData,setHourData]=useState([]);
  // Build/update 24h trend chart when location changes
  useEffect(() => {
    if (!location || !trendRef.current) return;
 
    const color = location.color;
 
    async function fetchAndRender() {
      let hourData = [];
 
      try {
        const response = await fetch('http://127.0.0.1:8000/location-data', {
          method: 'POST',
          headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
          body: JSON.stringify({ district: location.district, city: location.city }),
        });
        if (response.ok) {
          hourData = await response.json();
        }
      } catch (err) {
        console.error('Failed to fetch hour data:', err);
      }
 
      const labels = Array.from({ length: 24 }, (_, i) => `${i}:00`);
      const dt=hourData.map(it=>it['value'])
      // Destroy existing chart before creating a new one
      if (trendChart.current) {
        trendChart.current.destroy();
        trendChart.current = null;
      }
 
      trendChart.current = new ChartJS(trendRef.current, {
        type: 'line',
        data: {
          labels,
          datasets: [{
            label: 'AQI',
            data: dt,           // ← correct key
            borderColor: color,
            backgroundColor: color + '33',
            borderWidth: 2,
            pointRadius: 3,
            pointBackgroundColor: dt.map(aqiColor),
            fill: true,
            tension: 0.4,
          }],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: CHART_SCALE_STYLE,
        },
      });
    }
 
    fetchAndRender();
 
    // Cleanup: destroy chart when location changes or component unmounts
    return () => {
      if (trendChart.current) {
        trendChart.current.destroy();
        trendChart.current = null;
      }
    };
  }, [location]);

  // Build weekly pattern chart (static data — replace with real API data)
  useEffect(() => {
    if (!weekRef.current || weekChart.current) return;
    const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    async function fetchAndRender() {
      let weeklyData = [];
      try {
        const response = await fetch('http://127.0.0.1:8000/location/week-pattern', {
          method: 'POST',
          headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
          body: JSON.stringify({ district: location.district, city: location.city }),
        });
        if (response.ok) {
          weeklyData = await response.json();
        }
      } catch (err) {
        console.error('Failed to fetch hour data:', err);
      }
      // Destroy existing chart before creating a new one
      if (weekChart.current) {
        weekChart.current.destroy();
        weekChart.current = null;
      }
 
       weekChart.current = new ChartJS(weekRef.current, {
      type: 'bar',
      data: {
        labels: days,
        datasets: [{
          label: 'Avg AQI',
          data:weeklyData,
          backgroundColor: weeklyData.map(aqiColor),
          borderRadius: 6,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { font: { size: 10 }, color: '#64748b' }, grid: { display: false } },
          y: { ticks: { font: { size: 9  }, color: '#64748b' }, grid: { color: '#1e293b55' } },
        },
      },
    });
    }
 
    fetchAndRender();
     return () => {
      if (weekChart.current) {
        weekChart.current.destroy();
        weekChart.current = null;
      }
    };
   
  }, [location]);
  // Build pollutant doughnut chart (static data — replace with real API data)
  useEffect(() => {
     if (!location?.district || !location?.city || !pollRef.current) return;
     async function fetchAndRender() {
      let dt = [];
      try {
        const response = await fetch('http://127.0.0.1:8000/location/daily-pattern', {
          method: 'POST',
          headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
          body: JSON.stringify({ district: location.district, city: location.city }),
        });
        if (response.ok) {
          dt = await response.json();
          console.log(dt)
        }
      } catch (err) {
        console.error('Failed to fetch hour data:', err);
      }
      // Destroy existing chart before creating a new one
      if (pollChart.current) {
         pollChart.current.destroy();
         pollChart.current = null;
      }
    pollChart.current = new ChartJS(pollRef.current, {
      type: 'doughnut',
      data: {
        labels: ['PM 25','PM 10','O3','NO2','SO2','CO'],
        datasets: [{
          data: dt,
          backgroundColor: ['#ef4444', '#f97316', '#eab308', '#22c55e', '#3b82f6','#7bf5f9'],
          borderWidth: 0,
          hoverOffset: 6,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '65%',
        plugins: {
          legend: {
            position: 'right',
            labels: { font: { size: 10 }, color: '#94a3b8', padding: 8, boxWidth: 10 },
          },
        },
      },
    });
    }
 
    fetchAndRender();
     return () => {
      if (pollChart.current) {
        pollChart.current.destroy();
        pollChart.current = null;
      }}
    
  }, [location]);

  // Build/update district comparison chart
  useEffect(() => {
    if (!locations || !compRef.current) return;
    
     async function fetchAndRender() {
      let r_location = [];
 
      try {
        const response = await fetch('http://127.0.0.1:8000/location/rank');
        if (response.ok) {
          r_location = await response.json();
        }
      } catch (err) {
        console.error('Failed to fetch hour data:', err);
      }
 
      const names = r_location.map(l => l.name);
    const data  = r_location.map(l => l.aqi);
      // Destroy existing chart before creating a new one
      if (compChart.current) {
        compChart.current.destroy();
        compChart.current = null;
      }
      
        compChart.current = new ChartJS(compRef.current, {
        type: 'bar',
        data: {
          labels: names,
          datasets: [{
            label: 'AQI',
            data:data,
            backgroundColor: data.map(aqiColor),
            borderRadius: 4,
          }],
        },
        options: {
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { font: { size: 9 }, color: '#64748b' }, grid: { color: '#1e293b55' } },
            y: { ticks: { font: { size: 9 }, color: '#64748b' }, grid: { display: false } },
          },
        },
      });
    }
 
    fetchAndRender();
  }, [locations]);

  // Destroy all charts on unmount
  useEffect(() => {
    return () => {
      [trendChart, weekChart, pollChart, compChart].forEach(ref => {
        if (ref.current) { ref.current.destroy(); ref.current = null; }
      });
    };
  }, []);

  return (
    <div className="charts-grid">
      {/* 24h Trend */}
      <div className="chart-card">
        <div className="chart-hdr">
          <div>
            <div className="chart-title">24h AQI Trend</div>
            <div className="chart-sub">Today's hourly readings</div>
          </div>
          <span className="chart-badge chart-badge--live">Live</span>
        </div>
        <div className="canvas-wrap" style={{ height: 180 }}>
          <canvas ref={trendRef} role="img" aria-label="Line chart of AQI over 24 hours">24h AQI trend.</canvas>
        </div>
      </div>

      {/* Weekly Pattern */}
      <div className="chart-card">
        <div className="chart-hdr">
          <div>
            <div className="chart-title">Weekly Pattern</div>
            <div className="chart-sub">Average AQI by day</div>
          </div>
          <span className="chart-badge chart-badge--week">7 days</span>
        </div>
        <div className="canvas-wrap" style={{ height: 180 }}>
          <canvas ref={weekRef} role="img" aria-label="Bar chart of weekly average AQI">Weekly AQI pattern.</canvas>
        </div>
      </div>

      {/* Pollutant Breakdown */}
      <div className="chart-card">
        <div className="chart-hdr">
          <div>
            <div className="chart-title">Pollutant Breakdown</div>
            <div className="chart-sub">Composition today</div>
          </div>
          <span className="chart-badge chart-badge--poll">PM Analysis</span>
        </div>
        <div className="canvas-wrap" style={{ height: 180 }}>
          <canvas ref={pollRef} role="img" aria-label="Doughnut chart of pollutant composition">Pollutant composition.</canvas>
        </div>
      </div>

      {/* District Comparison */}
      <div className="chart-card">
        <div className="chart-hdr">
          <div>
            <div className="chart-title">District Comparison</div>
            <div className="chart-sub">Current AQI by zone</div>
          </div>
          <span className="chart-badge chart-badge--comp">All Zones</span>
        </div>
        <div className="canvas-wrap" style={{ height: 180 }}>
          <canvas ref={compRef} role="img" aria-label="Horizontal bar chart comparing AQI across districts">District AQI comparison.</canvas>
        </div>
      </div>
    </div>
  );
}