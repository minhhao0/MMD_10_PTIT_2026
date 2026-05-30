
export function aqiColor(value) {
  if (value <= 50)  return '#00E400';
  if (value <= 100) return '#FFFF00';
  if (value <= 150) return '#FF7E00';
  if (value <= 200) return '#FF0000' ;
  if (value<=300) return '#8F3F97';
  return '#7E0023';
}

export function aqiCategory(value) {
  if (value <= 50)  return"Tot";
  if (value <= 100) return "Trung binh";
  if (value <= 150) return "Kem";
  if (value <= 200) return "Xau";
  if(value<=300) return 'Rat Xau';
  return "Nguy Hai";
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