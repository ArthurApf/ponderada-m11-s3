import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import random
from datetime import datetime

def fetch_random_fire_pokemon():
    response = requests.get("https://pokeapi.co/api/v2/type/fire")
    if response.status_code != 200:
        raise Exception("Failed to fetch data from PokeAPI")
    
    pokemon_list = response.json()['pokemon']
    random_pokemon = random.choice(pokemon_list)['pokemon']
    
    pokemon_data = requests.get(random_pokemon['url']).json()
    return {
        'number': pokemon_data['id'],
        'name': pokemon_data['name'],
        'type': 'fire',
        'weight': pokemon_data['weight']
    }

def process_data():
    data = fetch_random_fire_pokemon()
    pokemon_name = data.get('name', 'unknown_pokemon')
    df = pd.DataFrame([data])
    filename = f"{pokemon_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filename)
    return filename

def prepare_dataframe_for_insert(df):
    df['data_ingestao'] = datetime.now()
    df['dado_linha'] = df.apply(lambda row: row.to_json(), axis=1)
    df['tag'] = 'example_tag'
    return df[['data_ingestao', 'dado_linha', 'tag']]
