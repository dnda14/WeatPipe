import pandas as pd
from extract import extraer_datos, CIUDADES
from transform import transformar_datos
from load import cargar


def main():
    data = []
    for ciudad in CIUDADES:
        print(f"\n=== Procesando {ciudad} ===")
        raw_data = extraer_datos(ciudad)
        print(f"Datos extraídos: {'✓' if raw_data else '✗'}")
        
        data_u = transformar_datos(raw_data)
        print(f"Datos transformados: {'✓' if data_u is not None else '✗'}")
        
        if data_u is not None and not data_u.empty:
            print(f"DataFrame agregado con {len(data_u)} filas")
            data.append(data_u)
        else:
            print(f"No se agregaron datos para {ciudad}")
    
    print(f"\n=== Total de DataFrames: {len(data)} ===")
    if data:
        # Properly concatenate all DataFrames
        df = pd.concat(data, ignore_index=True)
        print(f"\n=== DataFrame Final ({len(df)} filas) ===")
        print(df)           
        cargar(df)
    
if __name__ == "__main__":
    main()