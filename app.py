from flask import Flask, render_template, request
import joblib
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.metrics import MeanSquaredError
from tensorflow.keras.losses import MeanSquaredError as MSELoss
from datetime import timedelta

app = Flask(__name__)


# LOAD MODEL + DATA

model = load_model("lstm_model.h5", custom_objects={
    'mse': MeanSquaredError(),
    'keras.metrics.mse': MeanSquaredError()
})
scaler_x = joblib.load("scaler_x.pkl")
scaler_y = joblib.load("scaler_y.pkl")
df = joblib.load("data.pkl")

df['time'] = pd.to_datetime(df['time'])

# FEATURES 
features = [
    'pm10','carbon_monoxide','nitrogen_dioxide','sulphur_dioxide','ozone',
    
    'temperature_2m','relative_humidity_2m','wind_speed_10m','rain',
    
    'aqi_lag1','aqi_lag3','aqi_lag6','aqi_lag24',
    
    'hour','dayofweek'
]


# AQI LEVEL

def get_aqi_level(aqi):
    if aqi <= 50:
        return "Tốt", "success"
    elif aqi <= 100:
        return "Trung bình", "warning"
    elif aqi <= 150:
        return "Kém", "danger"
    else:
        return "Nguy hiểm", "dark"


# HOME

@app.route('/')
def home():
    cities = sorted(df['city'].unique())
    return render_template('index.html', cities=cities)


# LOAD DISTRICT

@app.route('/get_districts')
def get_districts():
    city = request.args.get('city')
    districts = sorted(df[df['city']==city]['district'].unique())
    return {"districts": districts}


# PREDICT 24H

@app.route('/predict')
def predict():
    city = request.args.get('city')
    district = request.args.get('district')

    df_filtered = df[(df['city']==city) & (df['district']==district)]

    if df_filtered.empty:
        return render_template("predict.html", error="Không có dữ liệu")

    df_filtered = df_filtered.sort_values('time')

    # lấy 24 giờ gần nhất
    last_24 = df_filtered.tail(24).copy()

    # tạo feature time
    last_24['hour'] = last_24['time'].dt.hour
    last_24['dayofweek'] = last_24['time'].dt.dayofweek

    # scale
    x = last_24[features].values
    x = scaler_x.transform(x)

    # reshape cho LSTM
    x = x.reshape(1, 24, len(features))

    # predict 24h
    preds = model.predict(x)[0]

    # inverse scale
    preds = scaler_y.inverse_transform(preds.reshape(-1,1)).flatten()

    # fix âm
    preds = np.clip(preds, 0, 300)

    results = []
    current_time = last_24['time'].iloc[-1]

    for i in range(24):
        current_time += timedelta(hours=1)

        aqi = float(preds[i])
        level, color = get_aqi_level(aqi)

        results.append({
            "time": current_time.strftime("%H:%M"),
            "aqi": round(aqi,2),
            "level": level,
            "color": color
        })

    return render_template(
        "predict.html",
        results=results,
        city=city,
        district=district,
        times=[r["time"] for r in results],
        aqi_values=[r["aqi"] for r in results]
    )


# RUN

if __name__ == "__main__":
    app.run(debug=True)