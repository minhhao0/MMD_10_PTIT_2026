export const LOCATIONS = [
  { name: 'Hoàn Kiếm',    coords: '21.0285° N, 105.8542° E', aqi: 142, min: 88,  max: 178, pm: 53.2, hum: 74, cluster: 'Cluster A', cx: 250, cy: 185 },
  { name: 'Đống Đa',      coords: '21.0245° N, 105.8412° E', aqi: 167, min: 110, max: 201, pm: 68.1, hum: 71, cluster: 'Cluster A', cx: 210, cy: 215 },
  { name: 'Cầu Giấy',     coords: '21.0316° N, 105.7953° E', aqi: 98,  min: 54,  max: 128, pm: 32.4, hum: 78, cluster: 'Cluster B', cx: 145, cy: 170 },
  { name: 'Tây Hồ',       coords: '21.0622° N, 105.8231° E', aqi: 44,  min: 18,  max: 67,  pm: 12.1, hum: 82, cluster: 'Cluster B', cx: 190, cy: 115 },
  { name: 'Hai Bà Trưng', coords: '21.0051° N, 105.8631° E', aqi: 189, min: 134, max: 212, pm: 78.9, hum: 69, cluster: 'Cluster C', cx: 278, cy: 245 },
  { name: 'Nam Từ Liêm',  coords: '21.0048° N, 105.7644° E', aqi: 76,  min: 42,  max: 105, pm: 24.6, hum: 76, cluster: 'Cluster B', cx: 108, cy: 255 },
];

export function aqiColor(value) {
  if (value <= 50)  return '#22c55e';
  if (value <= 100) return '#eab308';
  if (value <= 150) return '#f97316';
  if (value <= 200) return '#ef4444';
  return '#a855f7';
}

export function aqiCategory(value) {
  if (value <= 50)  return 'Good';
  if (value <= 100) return 'Moderate';
  if (value <= 150) return 'Sensitive';
  if (value <= 200) return 'Unhealthy';
  return 'Very Unhealthy';
}

export function gen24hData(baseAqi) {
  return Array.from({ length: 24 }, (_, i) =>
    Math.max(10, Math.round(
      baseAqi + Math.sin((i - 6) * Math.PI / 12) * 38 + (Math.random() - 0.5) * 18
    ))
  );
}

export function generateForecast(baseAqi, hours = 12) {
  return Array.from({ length: hours }, (_, i) => {
    const delta = (Math.random() - 0.4) * 35;
    const aqi = Math.max(10, Math.round(baseAqi + delta));
    return { hour: i + 1, aqi };
  });
}