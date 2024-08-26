import pytest
import json
from app import app

@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client

def test_receive_data(client, mocker):
    mocker.patch('data_pipeline.app.process_data', return_value='charmander_20240825120000.parquet')
    mocker.patch('data_pipeline.app.upload_file')
    mocker.patch('data_pipeline.app.download_file')
    mocker.patch('data_pipeline.app.prepare_dataframe_for_insert')
    mocker.patch('data_pipeline.app.insert_dataframe')

    response = client.post('/data', json={
        'date': 1596207620,
        'dados': 123
    })
    assert response.status_code == 200
    assert b'Dados recebidos, armazenados e processados com sucesso' in response.data
