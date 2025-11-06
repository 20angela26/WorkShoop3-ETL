import pandas as pd
import os

DATA_PATH = '../Data'  
OUTPUT_FILE = os.path.join(DATA_PATH, 'world_happiness_report.csv')


rename_dicts = {
    2015: {
        'Economy (GDP per Capita)': 'GDP per Capita',
        'Health (Life Expectancy)': 'Healthy life expectancy',
        'Trust (Government Corruption)': 'Perceptions of corruption',
        'Happiness Score': 'Happiness Score',
        'Happiness Rank': 'Happiness Rank',
        'Family': 'Social support'
    },
    2016: {
        'Economy (GDP per Capita)': 'GDP per Capita',
        'Health (Life Expectancy)': 'Healthy life expectancy',
        'Trust (Government Corruption)': 'Perceptions of corruption',
        'Happiness Score': 'Happiness Score',
        'Happiness Rank': 'Happiness Rank',
        'Family': 'Social support'
    },
    2017: {
        'Economy..GDP.per.Capita.': 'GDP per Capita',
        'Health..Life.Expectancy.': 'Healthy life expectancy',
        'Trust..Government.Corruption.': 'Perceptions of corruption',
        'Happiness.Score': 'Happiness Score',
        'Happiness.Rank': 'Happiness Rank',
        'Family': 'Social support'
    },
    2018: {
        'Country or region': 'Country',
        'Overall rank': 'Happiness Rank',
        'Score': 'Happiness Score',
        'GDP per capita': 'GDP per Capita',
        'Social support': 'Social support',
        'Healthy life expectancy': 'Healthy life expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Perceptions of corruption'
    },
    2019: {
        'Country or region': 'Country',
        'Overall rank': 'Happiness Rank',
        'Score': 'Happiness Score',
        'GDP per capita': 'GDP per Capita',
        'Social support': 'Social support',
        'Healthy life expectancy': 'Healthy life expectancy',
        'Freedom to make life choices': 'Freedom',
        'Perceptions of corruption': 'Perceptions of corruption'
    }
}


years = [2015, 2016, 2017, 2018, 2019]
common_cols = [
    'Country', 'Happiness Rank', 'Happiness Score',
    'GDP per Capita', 'Social support', 'Healthy life expectancy',
    'Freedom', 'Generosity', 'Perceptions of corruption', 'Year'
]


dfs = []

print("Iniciando lectura y transformación de los CSV desde '../Data/'...\n")

for year in years:
    file_path = os.path.join(DATA_PATH, f"{year}.csv")
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        continue

    print(f"Procesando: {file_path}")
    df = pd.read_csv(file_path)
    df.rename(columns=rename_dicts[year], inplace=True, errors='ignore')
    df['Year'] = year

    
    for col in common_cols:
        if col not in df.columns:
            df[col] = pd.NA


    df_clean = df[common_cols].copy()

    
    numeric_cols = df_clean.select_dtypes(include='number').columns
    df_clean[numeric_cols] = df_clean[numeric_cols].round(3)

    dfs.append(df_clean)

dataset_unificado = pd.concat(dfs, ignore_index=True)

print("\n Transformación completa.")
print(f"Total de registros unificados: {len(dataset_unificado)}")


dataset_unificado.to_csv(OUTPUT_FILE, index=False)
print(f"Dataset final guardado en: {OUTPUT_FILE}")
