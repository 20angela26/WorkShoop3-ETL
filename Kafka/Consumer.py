from kafka import KafkaConsumer
import json
import joblib
import mysql.connector
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()


db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)
cursor = db.cursor()


try:
    model = joblib.load("../Model/modelo_felicidad.pkl")
    print("Modelo cargado correctamente (modelo_felicidad.pkl)")
except Exception as e:
    print(f" Error al cargar el modelo: {e}")
    exit()

modelo_nombre = "modelo_felicidad.pkl"


consumer = KafkaConsumer(
    'topico_felicidad',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='felicidad-group'
)

print("Consumer conectado a Kafka y MySQL. Esperando datos...")


for msg in consumer:
    data = msg.value
    ts = datetime.now()


    campos = [
        "GDP per Capita", "Social support", "Healthy life expectancy",
        "Freedom", "Generosity", "Perceptions of corruption", "Happiness Score"
    ]
    for campo in campos:
        if data.get(campo) in [None, "", "NaN"]:
            data[campo] = 0.0


    cursor.execute("""
        INSERT INTO staging_felicidad (
            country, happiness_rank, happiness_score, gdp_per_capita,
            social_support, healthy_life_expectancy, freedom, generosity,
            perceptions_of_corruption, year, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["Country"], data["Happiness Rank"], data["Happiness Score"],
        data["GDP per Capita"], data["Social support"], data["Healthy life expectancy"],
        data["Freedom"], data["Generosity"], data["Perceptions of corruption"],
        data["Year"], ts
    ))
    db.commit()


    try:
        X_df = pd.DataFrame([{
            'GDP per Capita': data["GDP per Capita"],
            'Social support': data["Social support"],
            'Healthy life expectancy': data["Healthy life expectancy"],
            'Freedom': data["Freedom"],
            'Generosity': data["Generosity"],
            'Perceptions of corruption': data["Perceptions of corruption"],
            'Country': data["Country"],
            'Year': data["Year"]
        }])
        pred = float(model.predict(X_df)[0])
    except Exception as e:
        print(f" Error al predecir para {data['Country']} ({data['Year']}): {e}")
        continue

    diferencia = data["Happiness Score"] - pred


    cursor.execute("SELECT nombre_pais FROM dim_pais WHERE nombre_pais = %s", (data["Country"],))
    pais_result = cursor.fetchone()

    if not pais_result:
        cursor.execute("""
            INSERT INTO dim_pais (
                nombre_pais, gdp_per_capita, social_support,
                healthy_life_expectancy, freedom, generosity, perceptions_of_corruption
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (
            data["Country"], data["GDP per Capita"], data["Social support"],
            data["Healthy life expectancy"], data["Freedom"],
            data["Generosity"], data["Perceptions of corruption"]
        ))
        db.commit()


    cursor.execute("SELECT id_tiempo FROM dim_tiempo WHERE year = %s", (data["Year"],))
    tiempo_result = cursor.fetchone()

    if tiempo_result:
        id_tiempo = tiempo_result[0]
    else:
        cursor.execute("""
            INSERT INTO dim_tiempo (year, mes, trimestre, dia)
            VALUES (%s,%s,%s,%s)
        """, (data["Year"], ts.month, (ts.month - 1)//3 + 1, ts.day))
        db.commit()
        id_tiempo = cursor.lastrowid


    feature_vector = {
        "GDP per Capita": data["GDP per Capita"],
        "Social support": data["Social support"],
        "Healthy life expectancy": data["Healthy life expectancy"],
        "Freedom": data["Freedom"],
        "Generosity": data["Generosity"],
        "Perceptions of corruption": data["Perceptions of corruption"]
    }

    cursor.execute("""
        INSERT INTO dim_prediccion (
            modelo_nombre, happiness_score_predicho, gdp_per_capita, social_support,
            healthy_life_expectancy, freedom, generosity, perceptions_of_corruption,
            feature_vector_json, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        modelo_nombre, pred,
        data["GDP per Capita"], data["Social support"], data["Healthy life expectancy"],
        data["Freedom"], data["Generosity"], data["Perceptions of corruption"],
        json.dumps(feature_vector), ts
    ))
    db.commit()
    id_prediccion = cursor.lastrowid


    cursor.execute("""
        INSERT INTO hechos_felicidad (
            nombre_pais, id_tiempo, id_prediccion,
            happiness_rank, happiness_score_real, diferencia, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["Country"], id_tiempo, id_prediccion,
        data["Happiness Rank"], data["Happiness Score"], diferencia, ts
    ))
    db.commit()

    print(f"[✅] {data['Country']} ({data['Year']}) → Real: {data['Happiness Score']:.3f} | Predicho: {pred:.3f} | Diferencia: {diferencia:.3f}")
