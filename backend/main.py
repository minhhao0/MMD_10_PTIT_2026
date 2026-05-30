from fastapi import FastAPI,HTTPException
from fastapi.middleware.cors import  CORSMiddleware
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from pydantic import BaseModel

from api.location  import Location_Searcher

load_dotenv('../.env')
host=os.getenv('POSTGRES_HOST')
port=os.getenv('POSTGRES_PORT')
dbname=os.getenv('POSTGRES_DB')
user=os.getenv('POSTGRES_USER')
password=os.getenv('POSTGRES_PASS')
conn=psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host
)
cur=conn.cursor()
origins = [
    "http://localhost:3000",
]
app=FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
fetcher=Location_Searcher(conn)
class QueryObject(BaseModel):
    district:str
    city:str
@app.get("/location")
async def get_location():
   locations=fetcher.get_all_location()
   return locations
@app.post("/location-data")
async def get_location(item:QueryObject):
   data=fetcher.get_special_location(item.district,item.city)
   return data
@app.post("/location/week-pattern")
async def get_location(item:QueryObject):
   data=fetcher.get_weekly_pattern(item.district,item.city)
   return data
@app.get("/location/rank")
async def get_location():
   data=fetcher.get_district_rank()
   return data
@app.post("/location/daily-pattern")
async def get_location(item:QueryObject):
   data=fetcher.get_today_pattern(item.district,item.city)
   return data
