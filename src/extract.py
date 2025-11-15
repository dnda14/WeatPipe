import os
import json

import requests 
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
API_KEY = os.getenv("API_KEY")  


CIUDADES = ["Arequipa,PE","Lima,PE"]

def extraer_datos(ciudad):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"

    try:
        r = requests.get(url,timeout=10)
        r.raise_for_status()
        data=r.json()
        
        timestamp= datetime.now().strftime("%Y-%m-%d_%H%M%S")
        ciudad =ciudad.replace(",","_")
        path=f"data/raw/{ciudad}_{timestamp}.json"
        os.makedirs("data/raw",exist_ok=True)
        
        with open(path,"w") as f:
            json.dump(data,f,indent=4)
            
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"Erro al extraer datos para {ciudad}: {e}")
        return None