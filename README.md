# Atividade ponderada semana 3

#### Descrição: O código passado de exemplo na aula foi alterado para consumir uma API pública e fazer ingestão de dados vindos dessa API, a partir da rota "/data". Foi escolhido o PokeAPI, aplicado em uma função que seleciona um Pokemon aleatorio do tipo fogo, e fornece as caracteristicas: "number", "name", "type" e "weight", fora a data e hora de ingestão do dado.

## Execução:

Acesse o diretorio /scr
~~~
cd .\src\ 
~~~

Execute o docker-compose
~~~
docker-compose up --build -d 
~~~

Execute o app.py
~~~
poetry run python .\app.py
~~~

Execute a rota POST
~~~
http://localhost:5000/data
~~~

## Visualização de resultados

Após a execução da rota, pode-se obervar:

1. Bucket no minio: Acessando o localhost:9000, e em seguida aceesando o minio com o login e senha "minioadmin", pode-se visualizar o bucket "pokemons-tipo-fogo", com os dados dos pokemons chamados

<br>

2. Downloads: Ao executar a rota, a função executa um download com os dados, tendo o nome do pokemon escolhido, dentro da pasta /src.

<br>

3. Dbeaver: Basta realizar conexão local com o clickhouse (a partir do mesmo procedimento da aula), e rodar o script a seguir no localhost:

~~~
CREATE OR REPLACE VIEW working_data_view AS
SELECT
    data_ingestao,
    JSONExtractInt(dado_linha, 'number') AS number,
    JSONExtractString(dado_linha, 'name') AS name,
    JSONExtractString(dado_linha, 'type') AS type,
    JSONExtractInt(dado_linha, 'weight') AS weight,
    tag
FROM working_data;

~~~

Então no Dbeaver selecione "visualização", "working_data_view" e "Dados", nesta ordem.