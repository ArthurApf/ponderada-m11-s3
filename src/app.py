from flask import Flask, jsonify
from data_pipeline.minio_client import create_bucket_if_not_exists, upload_file, download_file
from data_pipeline.clickhouse_client import execute_sql_script, get_client, insert_dataframe
from data_pipeline.data_processing import process_data, prepare_dataframe_for_insert
import pandas as pd


app = Flask(__name__)

# Criar bucket se não existir
create_bucket_if_not_exists("pokemons-tipo-fogo")

# Executar o script SQL para criar a tabela
execute_sql_script('sql/create_table.sql')

@app.route('/data', methods=['POST'])
def receive_data():
    try:
        filename = process_data()
        upload_file("pokemons-tipo-fogo", filename)

        download_file("pokemons-tipo-fogo", filename, f"downloaded_{filename}")
        df_parquet = pd.read_parquet(f"downloaded_{filename}")

        df_prepared = prepare_dataframe_for_insert(df_parquet)
        client = get_client()
        insert_dataframe(client, 'working_data', df_prepared)

        return jsonify({"message": "Dados do Pokémon recebidos, armazenados e processados com sucesso"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
