import psycopg2
from dotenv import load_dotenv
import os
import pandas  as pd
from tqdm import tqdm

class Location_Searcher:
    def __init__(self,conn):
        self.conn=conn
        self.cur=conn.cursor()
    def get_all_location(self):
        query='SELECT * FROM aqi_readings order by event_time desc limit 686'
        self.cur.execute(query)
        result = self.cur.fetchall()
        return [{'city':item[1],'district':item[2], 'lat':item[4],'lon':item[5],'event_time':item[6],'pm_25':item[8],'aqi_final':item[22],'label':item[23],'color':item[24],'advice':item[25]} for item in result ]
    def get_special_location(self,district,city):
        query='SELECT * FROM aqi_readings where province= %s and district=%s and event_time::date=current_date order by event_time asc limit 24'
        params=(city,district)
        self.cur.execute(query,params)
        results=self.cur.fetchall()
        return [{'label':"{:02d}:00".format(int(item[6].hour)),'value': item[22]} for item in results]
    def get_weekly_pattern(self,district,city):
        query="SELECT dow,avg(aqi_final) as aqi_avg from (SELECT aqi_final,EXTRACT(DOW from event_time) as dow from aqi_readings where district=%s and province=%s) group by dow"
        param=(district,city)
        self.cur.execute(query,param)
        results=self.cur.fetchall()
        results={int(it[0]):int(it[1]) for it in results}
        final=[]
        for i in range(1,7):
            if i in results.keys():
                final.append(results[i])
            else:
                final.append(0)
        if 0 in results.keys():
            final.append(results[0])
        return final
    def get_today_pattern(self,district,city):
        query='SELECT aqi_pm25,aqi_pm10,aqi_o3,aqi_no2,aqi_so2,aqi_co FROM aqi_readings where event_time::date=current_date and district=%s and  province=%s order by event_time desc limit 1 '
        param=(district,city)
        self.cur.execute(query,param)
        rs=self.cur.fetchall()
        total=sum(rs[0])
        print(total)
        return [round((it/total)*100) for it in rs[0]]
    def get_district_rank(self):
        query='Select * from aqi_by_province limit 10'
        self.cur.execute(query)
        res=self.cur.fetchall()
        return [{'name':it[0],'aqi':int(it[3])} for it in res]
# if __name__=="__main__":
#     load_dotenv('../../.env')
#     host = os.getenv('POSTGRES_HOST')
#     port = os.getenv('POSTGRES_PORT')
#     dbname = os.getenv('POSTGRES_DB')
#     user = os.getenv('POSTGRES_USER')
#     password = os.getenv('POSTGRES_PASS')
#     conn = psycopg2.connect(
#         dbname=dbname,
#         user=user,
#         password=password,
#         host=host
#     )
#     cur = conn.cursor()
#     fetcher=Location_Searcher(conn)
#     # q="SELECT district,event_time from aqi_readings where province='Ha Noi' and event_time::date=current_date"
#     # cur.execute(q)
#     # rs=cur.fetchall()
#     # print(rs)
#     print(fetcher.get_today_pattern('Thanh Xuan','Ha Noi'))
#     # print(fetcher.get_district_rank())
