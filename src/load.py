from sqlalchemy import create_engine

def cargar(df):
    engine = create_engine('sqlite:///clima.db', echo=True)
    df.to_sql('clima', con=engine, if_exists='append', index = False)
    