import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")  


CIUDADES = "Arequipa,PE"

def extraer_datos(ciudad):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
    print(url)
    r = requests.get(url)
    print(f"Status Code: {r.status_code}")
    print(f"Respuesta: {r.text}")
    return r.json()

def transformar_datos(json_data):
    return {
        "Ciudad": json_data.get("name","NN"),
        "Temperatura" : json_data["main"]["temp"],
        "Humedad":json_data["main"]["humidity"],
        "Description":json_data["weather"][0]["description"],
        "Viento":json_data["wind"]["speed"]
    }
    
def cargar(df):
    engine = create_engine('sqlite:///clima.db', echo=True)
    df.to_sql('clima', con=engine, if_exists='replace', index = False)
    
def main():
    data = [transformar_datos(extraer_datos(CIUDADES))]
    df = pd.DataFrame(data)
    print(df)
    cargar(df)
    
if __name__ == "__main__":
    main()