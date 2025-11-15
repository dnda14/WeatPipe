import pandas as pd

from extract import extraer_datos, CIUDADES
from transform import transformar_datos
from load import cargar


def main():
    data = []
    for ciudad in CIUDADES:
          
        data_u = transformar_datos(extraer_datos(ciudad))
        
        if data_u:
            data.append(data_u)
    
    if data:
        df = pd.DataFrame(data)
        
        print(df)
        cargar(df)
    
if __name__ == "__main__":
    main()