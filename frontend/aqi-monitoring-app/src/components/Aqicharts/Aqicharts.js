import React, { useEffect, useRef } from 'react';
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

  // Build/update 24h trend chart when location changes
  useEffect(() => {
    if (!location || !trendRef.current) return;
    const data   = gen24hData(location.aqi);
    const labels = Array.from({ length: 24 }, (_, i) => (i % 4 === 0 ? `${i}:00` : ''));
    const color  = aqiColor(location.aqi);

    if (trendChart.current) {
      trendChart.current.data.datasets[0].data             = data;
      trendChart.current.data.datasets[0].borderColor      = color;
      trendChart.current.data.datasets[0].backgroundColor  = color + '33';
      trendChart.current.update();
    } else {
      trendChart.current = new ChartJS(trendRef.current, {
        type: 'line',
        data: {
          labels,
          datasets: [{
            label: 'AQI',
            data,
            borderColor: color,
            backgroundColor: color + '33',
            borderWidth: 2,
            pointRadius: data.map((_, i) => (i % 4 === 0 ? 3 : 0)),
            pointBackgroundColor: data.map(aqiColor),
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
  }, [location]);

  // Build weekly pattern chart (static data — replace with real API data)
  useEffect(() => {
    if (!weekRef.current || weekChart.current) return;
    const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
    const data = [138, 155, 162, 145, 168, 102, 79];
    weekChart.current = new ChartJS(weekRef.current, {
      type: 'bar',
      data: {
        labels: days,
        datasets: [{
          label: 'Avg AQI',
          data,
          backgroundColor: data.map(aqiColor),
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
  }, []);

  // Build pollutant doughnut chart (static data — replace with real API data)
  useEffect(() => {
    if (!pollRef.current || pollChart.current) return;
    pollChart.current = new ChartJS(pollRef.current, {
      type: 'doughnut',
      data: {
        labels: ['PM2.5', 'PM10', 'NO₂', 'O₃', 'CO'],
        datasets: [{
          data: [38, 24, 18, 12, 8],
          backgroundColor: ['#ef4444', '#f97316', '#eab308', '#22c55e', '#3b82f6'],
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
  }, []);

  // Build/update district comparison chart
  useEffect(() => {
    if (!locations || !compRef.current) return;
    const names = locations.map(l => l.name);
    const data  = locations.map(l => l.aqi);

    if (compChart.current) {
      compChart.current.data.labels                        = names;
      compChart.current.data.datasets[0].data             = data;
      compChart.current.data.datasets[0].backgroundColor  = data.map(aqiColor);
      compChart.current.update();
    } else {
      compChart.current = new ChartJS(compRef.current, {
        type: 'bar',
        data: {
          labels: names,
          datasets: [{
            label: 'AQI',
            data,
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