import pytest
import pandas as pd
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert

def test_process_data(mocker):
    mock_fetch_random_fire_pokemon = mocker.patch('data_pipeline.data_processing.fetch_random_fire_pokemon')
    mock_fetch_random_fire_pokemon.return_value = {
        'name': 'charmander',
        'number': 4,
        'type': 'fire',
        'weight': 85
    }
    
    filename = process_data()
    assert filename.startswith('charmander_')
    df = pd.read_parquet(filename)
    assert 'name' in df.columns
    assert df['name'][0] == 'charmander'

def test_prepare_dataframe_for_insert():
    df = pd.DataFrame({
        'data': [1],
        'dados': [2]
    })
    df_prepared = prepare_dataframe_for_insert(df)
    assert 'data_ingestao' in df_prepared.columns
    assert 'dado_linha' in df_prepared.columns
    assert 'tag' in df_prepared.columns
