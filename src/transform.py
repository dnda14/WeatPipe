import pandas as pd

def transformar_datos(json_data):
    if json_data is None:
        return None
    try:
        df = pd.DataFrame([{
            "Ciudad": json_data.get("name", "NN"),
            "Temperatura": json_data["main"]["temp"],
            "Humedad": json_data["main"]["humidity"],
            "Description": json_data["weather"][0]["description"],
            "Viento": json_data["wind"]["speed"]
        }])
        return df
    except KeyError:
        print("Error al transformar la data")
        return None