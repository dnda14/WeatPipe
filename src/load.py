from sqlalchemy import create_engine

def cargar(df):
    engine = create_engine('sqlite:///clima.db', echo=True)
    df.to_sql('clima', con=engine, if_exists='append', index = False)
def cargar2(df):
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
        echo=True
    )
    df.to_sql('clima', con=engine, if_exists='append', index=False)