import os
import json
import logging

import boto3
import requests 
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
API_KEY = os.getenv("API_KEY")  

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CIUDADES = ["Arequipa,PE","Lima,PE"]

def subir_a_s3(data, ciudad, timestamp):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        now = datetime.now()
        s3_key = f"raw/{now.strftime('%Y/%m/%d')}/{ciudad}_{timestamp}.json"
        
        s3.put_object(
            Bucket=AWS_S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )
        
        logging.info(f"Uploaded to S3: s3://{AWS_S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"Error uploading to S3 for {ciudad}: {e}")

def extraer_datos(ciudad):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"

    try:
        r = requests.get(url,timeout=10)
        r.raise_for_status()
        data=r.json()
        
        timestamp= datetime.now().strftime("%Y-%m-%d_%H%M%S")
        ciudad_clean = ciudad.replace(",","_")
        
        # Guardar localmente
        path=f"data/raw/{ciudad_clean}_{timestamp}.json"
        os.makedirs("data/raw",exist_ok=True)
        with open(path,"w") as f:
            json.dump(data,f,indent=4)
        
        subir_a_s3(data, ciudad_clean, timestamp)
            
        return data
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al extraer datos para {ciudad}: {e}")
        return None