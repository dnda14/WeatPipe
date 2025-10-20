import requests
import pandas as pd
from sqlalchemy import create_engine

API_KEY = "a3412d78914c8dc3bc75717690edd500"
CIUDADES = "Arequipa,PE"

def extraer_datos(ciudad):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid=[API_KEY]&units=metric"
    
    r = requests.get(url)
    return r.json()

def transformar_datos(json_data):
    return {
        "Ciudad": json_data["name"],
        "Temperatura" : json_data["main"]["temp"],
        "Humedad":json_data["main"]["humidity"],
        "Description":json_data["weather"][0]["description"],
        "viento":json_data["wind"]["speed"]
    }
    
def cargar(df):
    engine = create_engine('sqlite:///clima.db', echo=True)
    df.to_sql('clima', con=engine, if_exists='repalce', index = False)
    
def main():
    data = [transformar_datos(extraer_datos(CIUDADES))]
    df = pd.DataFrame(data)
    print(df)
    cargar(df)
    
if __name__ == "__main__":
    main()