from kafka import KafkaConsumer
import json
import joblib
import mysql.connector
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os

# ===============================================================
# 🔧 CONFIGURACIÓN
# ===============================================================
load_dotenv()

db = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)
cursor = db.cursor()

# ===============================================================
# 🧠 CARGAR MODELO
# ===============================================================
try:
    modelo_nombre = "modelo_felicidad.pkl"
    model = joblib.load(f"../Model/{modelo_nombre}")
    print(f"✅ Modelo '{modelo_nombre}' cargado correctamente")
except Exception as e:
    print(f"❌ Error al cargar el modelo: {e}")
    exit()

# ===============================================================
# 🔌 KAFKA CONSUMER
# ===============================================================
consumer = KafkaConsumer(
    'topico_felicidad',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',           # ✅ SOLO MENSAJES NUEVOS
    enable_auto_commit=True,
    group_id='felicidad-group-v3'        # ⚠️ NUEVO GRUPO PARA EVITAR OFFSET ANTIGUOS
)

print("🟢 Consumer conectado a Kafka y MySQL. Esperando datos...\n")

# ===============================================================
# 🚀 PROCESAR MENSAJES
# ===============================================================
for msg in consumer:
    data = msg.value
    ts = datetime.now()

    # Validar valores nulos
    campos = [
        "GDP per Capita", "Social support", "Healthy life expectancy",
        "Freedom", "Generosity", "Perceptions of corruption", "Happiness Score"
    ]
    for campo in campos:
        if data.get(campo) in [None, "", "NaN"]:
            data[campo] = 0.0

    # ===============================================================
    # 1️⃣ Guardar datos reales
    # ===============================================================
    cursor.execute("""
        INSERT INTO staging_felicidad (
            country, year, happiness_rank, happiness_score, gdp_per_capita,
            social_support, healthy_life_expectancy, freedom, generosity,
            perceptions_of_corruption, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            happiness_rank = VALUES(happiness_rank),
            happiness_score = VALUES(happiness_score),
            gdp_per_capita = VALUES(gdp_per_capita),
            social_support = VALUES(social_support),
            healthy_life_expectancy = VALUES(healthy_life_expectancy),
            freedom = VALUES(freedom),
            generosity = VALUES(generosity),
            perceptions_of_corruption = VALUES(perceptions_of_corruption),
            timestamp = VALUES(timestamp)
    """, (
        data["Country"], data["Year"], data["Happiness Rank"], data["Happiness Score"],
        data["GDP per Capita"], data["Social support"], data["Healthy life expectancy"],
        data["Freedom"], data["Generosity"], data["Perceptions of corruption"], ts
    ))
    db.commit()

    # ===============================================================
    # 2️⃣ Generar predicción
    # ===============================================================
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
        print(f"⚠️ Error al predecir {data['Country']} ({data['Year']}): {e}")
        continue

    # ===============================================================
    # 3️⃣ Guardar predicción
    # ===============================================================
    cursor.execute("""
        INSERT INTO prediccion_felicidad (
            country, year, modelo_nombre, happiness_score_predicho,
            gdp_per_capita, social_support, healthy_life_expectancy,
            freedom, generosity, perceptions_of_corruption, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            happiness_score_predicho = VALUES(happiness_score_predicho),
            timestamp = VALUES(timestamp)
    """, (
        data["Country"], data["Year"], modelo_nombre, pred,
        data["GDP per Capita"], data["Social support"],
        data["Healthy life expectancy"], data["Freedom"],
        data["Generosity"], data["Perceptions of corruption"], ts
    ))
    db.commit()

    # ===============================================================
    # 4️⃣ Guardar comparación
    # ===============================================================
    cursor.execute("""
        INSERT INTO comparacion_felicidad (
            country, year, modelo_nombre,
            happiness_score_real, happiness_score_predicho, timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            happiness_score_real = VALUES(happiness_score_real),
            happiness_score_predicho = VALUES(happiness_score_predicho),
            timestamp = VALUES(timestamp)
    """, (
        data["Country"], data["Year"], modelo_nombre,
        data["Happiness Score"], pred, ts
    ))
    db.commit()

    # ===============================================================
    # 5️⃣ Mostrar resultados
    # ===============================================================
    diferencia = pred - data["Happiness Score"]
    print(f"[{ts.strftime('%Y-%m-%d %H:%M:%S')}] ✅ {data['Country']} ({data['Year']}) "
            f"Modelo: {modelo_nombre} | Real: {data['Happiness Score']:.3f} | "
            f"Predicho: {pred:.3f} | Diferencia: {diferencia:.3f}")
