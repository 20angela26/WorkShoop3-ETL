import pandas as pd
import json
from kafka import KafkaProducer
import time
import os


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


data_path = "../Data"  


rename_dicts = {
    2015: {
        'Country': 'Country',
        'Happiness Rank': 'Happiness Rank',
        'Happiness Score': 'Happiness Score',
        'Economy (GDP per Capita)': 'GDP per Capita',
        'Family': 'Social support',
        'Health (Life Expectancy)': 'Healthy life expectancy',
        'Freedom': 'Freedom',
        'Trust (Government Corruption)': 'Perceptions of corruption',
        'Generosity': 'Generosity'
    },
    2016: {
        'Country': 'Country',
        'Happiness Rank': 'Happiness Rank',
        'Happiness Score': 'Happiness Score',
        'Economy (GDP per Capita)': 'GDP per Capita',
        'Family': 'Social support',
        'Health (Life Expectancy)': 'Healthy life expectancy',
        'Freedom': 'Freedom',
        'Trust (Government Corruption)': 'Perceptions of corruption',
        'Generosity': 'Generosity'
    },
    2017: {
        'Country': 'Country',
        'Happiness.Rank': 'Happiness Rank',
        'Happiness.Score': 'Happiness Score',
        'Economy..GDP.per.Capita.': 'GDP per Capita',
        'Family': 'Social support',
        'Health..Life.Expectancy.': 'Healthy life expectancy',
        'Freedom': 'Freedom',
        'Trust..Government.Corruption.': 'Perceptions of corruption',
        'Generosity': 'Generosity'
    },
    2018: {
        'Country or region': 'Country',
        'Overall rank': 'Happiness Rank',
        'Score': 'Happiness Score',
        'GDP per capita': 'GDP per Capita',
        'Social support': 'Social support',
        'Healthy life expectancy': 'Healthy life expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Perceptions of corruption',
        'Generosity': 'Generosity'
    },
    2019: {
        'Country or region': 'Country',
        'Overall rank': 'Happiness Rank',
        'Score': 'Happiness Score',
        'GDP per capita': 'GDP per Capita',
        'Social support': 'Social support',
        'Healthy life expectancy': 'Healthy life expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Perceptions of corruption',
        'Generosity': 'Generosity'
    }
}

common_cols = [
    'Country', 'Happiness Rank', 'Happiness Score',
    'GDP per Capita', 'Social support', 'Healthy life expectancy',
    'Freedom', 'Generosity', 'Perceptions of corruption', 'Year'
]

years = [2015, 2016, 2017, 2018, 2019]

print("Iniciando Producer: extracción, transformación y envío a Kafka...")

for year in years:
    file_path = f"{data_path}/{year}.csv"
    if not os.path.exists(file_path):
        print(f"⚠️ Archivo no encontrado: {file_path}")
        continue

    df = pd.read_csv(file_path)
    df.rename(columns=rename_dicts[year], inplace=True, errors='ignore')
    df['Year'] = year


    for col in common_cols:
        if col not in df.columns:
            df[col] = None
    df = df[common_cols]

    numeric_cols = df.select_dtypes(include='number').columns
    df[numeric_cols] = df[numeric_cols].round(3)

    for _, row in df.iterrows():
        message = row.to_dict()
        message = {k: (None if pd.isna(v) else v) for k, v in message.items()}
        producer.send('topico_felicidad', value=message)  # 👈 mismo tópico del consumer
        print(f"📤 Enviado: {message['Country']} - {year} | Score: {message['Happiness Score']}")
        time.sleep(0.05)

    time.sleep(0.5)  
producer.flush()
producer.close()
print("✅ Todos los datos enviados a Kafka exitosamente.")
