from flask import Flask, render_template
import joblib
import pandas as pd
from datetime import timedelta

app = Flask(__name__)

# load model
model = joblib.load('aqi_model.pkl')
scaler = joblib.load('scaler.pkl')
df = joblib.load('data.pkl')


# HOME
@app.route('/')
def home():
    return render_template('index.html')

# HÀM ĐÁNH GIÁ AQI
def get_aqi_level(aqi):
    if aqi <= 50:
        return "Tốt", "success"
    elif aqi <= 100:
        return "Trung bình", "warning"
    elif aqi <= 150:
        return "Kém", "danger"
    else:
        return "Nguy hiểm", "dark"

# PREDICT 3 DAYS
@app.route('/predict')
def predict_3days():
    df_sorted = df.sort_values('date')
    latest = df_sorted.iloc[-1]

    results = []
    current = latest.copy()
    current_date = pd.to_datetime(latest['date'])

    for i in range(3):
        current_date += timedelta(days=1)

        data = [[
            current['pm10'],
            current['carbon_monoxide'],
            current['nitrogen_dioxide'],
            current['sulphur_dioxide'],
            current['ozone'],
            current['temperature_2m'],
            current['relative_humidity_2m'],
            current['wind_speed_10m'],
            current['aqi_lag1'],
            current['aqi_lag2'],
            current['aqi_lag3']
        ]]

        data_scaled = scaler.transform(data)
        pred = model.predict(data_scaled)[0]

        level, color = get_aqi_level(pred)

        results.append({
            "date": current_date.strftime("%d/%m/%Y"),
            "aqi": round(pred, 2),
            "level": level,
            "color": color
        })

        # update lag chuẩn
        current['aqi_lag3'] = current['aqi_lag2']
        current['aqi_lag2'] = current['aqi_lag1']
        current['aqi_lag1'] = pred

    return render_template('predict.html', results=results)

# RUN
if __name__ == '__main__':
    app.run(debug=True)