import pytest
from unittest.mock import patch, MagicMock
from data_pipeline.clickhouse_client import execute_sql_script, insert_dataframe, create_view

@patch('data_pipeline.clickhouse_client.get_client')
def test_execute_sql_script(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    execute_sql_script('sql/test_script.sql')
    mock_client.command.assert_called_once()

@patch('data_pipeline.clickhouse_client.get_client')
def test_insert_dataframe(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    df = pd.DataFrame({'data': [1]})
    insert_dataframe(mock_client, 'test_table', df)
    mock_client.insert_df.assert_called_once_with('test_table', df)

@patch('data_pipeline.clickhouse_client.get_client')
def test_create_view(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    create_view('sql/test_view.sql')
    mock_client.command.assert_called_once()
