# ETLClim: Weather Data Engineering Pipeline ☁️📊

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.9.2-017CEE.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-336791.svg)
![dbt](https://img.shields.io/badge/dbt-1.8.0-FF694B.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)
![AWS S3](https://img.shields.io/badge/AWS-S3-569A31.svg)

## 📌 Descripción del Proyecto
ETLClim es un pipeline de datos (ETL/ELT) automatizado diseñado para extraer información meteorológica en tiempo real mediante la API de OpenWeather. El proyecto demuestra habilidades avanzadas de Data Engineering, implementando una arquitectura moderna que combina un **Data Lake** alojado en AWS S3 con un **Data Warehouse** estructurado en PostgreSQL, todo transformado mediante dbt y orquestado con Apache Airflow.

---

## 🏗️ Arquitectura de Datos (Data Lakehouse)

El flujo de datos sigue un enfoque modular, asegurando la inmutabilidad de los datos crudos y la escalabilidad del modelo analítico:

1. **Extracción (Extract)**: 
   - Scripts en Python consumen la API REST de OpenWeather para ciudades específicas.
2. **Carga Inicial y Respaldo (Load & Data Lake)**: 
   - El payload JSON crudo se sube automáticamente a un **bucket de AWS S3** (`boto3`) particionado por fecha (`raw/YYYY/MM/DD/`), conformando la capa *Bronze* del Data Lake.
   - Simultáneamente, los datos estructurados iniciales se cargan en la tabla `raw_clima` en PostgreSQL (`Landing Zone`).
3. **Validación (Data Quality)**:
   - Evaluaciones lógicas usando `Pandas` aseguran que las métricas extraídas sean congruentes (temperaturas posibles, humedad válida) antes de tocar la base de datos.
4. **Transformación (Transform - dbt)**: 
   - **dbt (Data Build Tool)** toma el control dentro del Data Warehouse, aplicando el modelo **Estrella (Star Schema)**.
   - Crea dimensiones (`dim_ciudades`) y tablas de hechos (`fct_clima`) listas para el análisis y herramientas de BI.
5. **Orquestación**:
   - Todo el proceso está programado y monitoreado gráficamente por **Apache Airflow** mediante un DAG.

---

## 🛠️ Tecnologías Empleadas
- **Orquestación:** Apache Airflow
- **Lenguaje Core:** Python (Pandas, requests, python-dotenv)
- **Data Warehouse:** PostgreSQL
- **Transformación / Modelado:** dbt (Data Build Tool)
- **Data Lake (Almacenamiento Cloud):** AWS S3 (SDK boto3)
- **Contenedorización:** Docker y Docker Compose

---

## 🚀 Instalación y Despliegue Local

Este proyecto está completamente dockerizado para asegurar su reproducibilidad técnica en cualquier entorno.

### Prerequisitos
- [Docker](https://www.docker.com/) y Docker Compose instalados.
- Cuenta gratuita en [OpenWeather](https://openweathermap.org/) (para obtener la API Key).
- Cuenta en AWS (para obtener credenciales IAM y crear un bucket S3).

### Pasos

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/TU_USUARIO/ETLClim.git
   cd ETLClim
   ```

2. **Configurar Variables de Entorno:**
   Crea un archivo `.env` en el directorio raíz basándote en el archivo de ejemplo (si existe) o pega este esquema:
   ```env
   API_KEY=tu_api_key_de_openweather
   AIRFLOW_SECRET_KEY=clave_secreta_aleatoria
   AWS_ACCESS_KEY_ID=tu_aws_access_key
   AWS_SECRET_ACCESS_KEY=tu_aws_secret_key
   AWS_REGION=us-east-2
   AWS_S3_BUCKET=tu-nombre-de-bucket-s3
   ```

3. **Iniciación de la Base de Datos y Creación de Usuarios Airflow:**
   Para inicializar la BD de Airflow por primera vez:
   ```bash
   docker-compose up airflow-init
   ```

4. **Levantar todos los contenedores:**
   ```bash
   docker-compose up -d --build
   ```

5. **Acceder a la interfaz de Airflow:**
   - Abre tu navegador en: `http://localhost:8080/`
   - **Usuario:** `airflow`
   - **Contraseña:** `airflow`
   - Activa el DAG `etl_clima_dag` usando el botón *toggle*.

---

## 📊 Estructura del Data Warehouse (dbt)

El modelado en `dbt` convierte los datos de la capa *raw* a esquemas optimizados para BI:

- **`stg_clima` (Staging):** Estandarización de nombres de columnas de `raw_clima` a tipos apropiados.
- **`dim_ciudades` (Dimensión):** Tabla con IDs únicos y nombres de ciudades extraídas.
- **`fct_clima` (Hechos):** Tabla principal que contiene las métricas continuas (temperatura, velocidad del viento, humedad) haciendo referencia al `ciudad_id`.

---

## 🗂️ Estructura del Proyecto

```text
ETLClim/
├── dags/                  # DAGs de Apache Airflow
│   └── etl_clima_dag.py
├── dbt_clima/             # Proyecto y modelos de dbt
│   ├── models/            
│   │   ├── staging/       # stg_clima
│   │   └── marts/         # dim_ciudades, fct_clima
│   ├── dbt_project.yml
│   └── profiles.yml       # Configuración de conexión de dbt a Postgres
├── src/                   # Lógica de extracción, carga y validación en Python
│   ├── extract.py         # Extracción de API y carga a S3 (boto3)
│   ├── transform.py       # Data Quality con Pandas
│   └── load.py            # Carga en PostgreSQL (`raw_clima`)
├── docker-compose.yaml    # Configuración principal de contenedores
├── Dockerfile             # Imagen extendida de Airflow (instala git y dbt)
├── requirements.txt       # Dependencias de Python (boto3, dbt-postgres, etc)
└── webserver_config.py    # Configuración de seguridad para Flask/Airflow
```
