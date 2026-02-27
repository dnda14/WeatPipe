import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transformar_datos(json_data):
    if json_data is None:
        return None
    try:
        ciudad = json_data.get("name")
        temperatura = json_data["main"]["temp"]
        humedad = json_data["main"]["humidity"]
        
        # --- Data Quality Checks ---
        if not ciudad:
            logging.error("Data Quality FAILED: City name is empty.")
            return None
        if not (-50 <= temperatura <= 60):
            logging.error(f"Data Quality FAILED for {ciudad}: Temperature {temperatura}°C is out of realistic bounds (-50 to 60).")
            return None
        if not (0 <= humedad <= 100):
            logging.error(f"Data Quality FAILED for {ciudad}: Humidity {humedad}% is out of bounds (0 to 100).")
            return None
        # ---------------------------

        df = pd.DataFrame([{
            "Ciudad": ciudad,
            "Temperatura": temperatura,
            "Humedad": humedad,
            "Description": json_data["weather"][0]["description"],
            "Viento": json_data["wind"]["speed"]
        }])
        
        logging.info(f"Data successfully transformed and validated for {ciudad}.")
        return df
    except KeyError as e:
        logging.error(f"Error transforming data: missing key {e}")
        return None