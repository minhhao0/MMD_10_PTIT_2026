from kafka import KafkaProducer
from datetime import datetime
import time
import json
import requests
kafka_bootstrap_servers='localhost:9092'
kafka_topic_name='test-topic'
producer=KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,api_version=(0,11,5),
                       value_serializer=lambda x:json.dumps(x).encode('utf-8'))
json_message=None
city_name=None
district_name=None
lat=10.7761
lon=106.6958
air_quality_api='https://air-quality-api.open-meteo.com/v1/air-quality'
aqi_params={
    'latitude':lat,
    'longitude':lon,
    'hourly':'european_aqi,us_aqi,pm10,pm2_5,nitrogen_dioxide,carbon_monoxide,sulphur_dioxide,ozone,aerosol_optical_depth,dust,uv_index,uv_index_clear_sky',
    'start_date':'2016-01-01',
    'end_date':'2026-02-02',
    'timezone':'Asia/Bangkok'
}
def get_aqi_detail(district_name,city_name,lat,lon):
    response=requests.get(air_quality_api,params=aqi_params).json()
    json_message={
        'city_name':city_name,
        'district':district_name,
        'latitude':lat,
        'longitude':lon,
        'timezone':response['timezone'],
        'timezone_abbreviation':response['timezone_abbreviation'],
        'hourly_units':response['hourly_units'],
        'hourly':response['hourly']
    }
    return json_message

while True:
    json_message=get_aqi_detail(district_name='Hoan Kiem',city_name="Ha Noi",lat=lat,lon=lon)
    producer.send(kafka_topic_name,json_message)
    print('Published message 1: ')