import time

import pandas as pd

from extract import extraer_datos, CIUDADES
from transform import transformar_datos
from load import cargar


def main():
    data = []
    for ciudad in CIUDADES:
        print(f"\n=== Processing {ciudad} ===")
        raw_data = extraer_datos(ciudad)
        print(f"data extracted: {'✓' if raw_data else '✗'}")
        
        data_u = transformar_datos(raw_data)
        print(f"data transformed: {'✓' if data_u is not None else '✗'}")
        
        if data_u is not None and not data_u.empty:
            print(f"DataFrame added with {len(data_u)} rows")
            data.append(data_u)
        else:
            print(f"no data added to {ciudad}")
    
    print(f"\n=== dataframe total: {len(data)} ===")
    if data:
        df = pd.concat(data, ignore_index=True)
        print(f"\n=== DataFrame Final ({len(df)} rows) ===")
        print(df)           
        cargar(df)
    
if __name__ == "__main__":
    while True:
        print("\n=== Run ETL ===")
        main()
        print("waiting 1 hour...\n")
        time.sleep(3600)