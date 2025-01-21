# Nestle Project

## Descrição
Este projeto é desenvolvido para para o Case Técnico Nestlé.

Os requisitos estão descritos no arquivo: 
./CASE ENGENHEIRO DE DADOS.pdf

## Instalação
Para instalar as dependências do projeto, execute o seguinte comando:
```sh
pip install -r requirements.txt
```

## Excecução
Para inicializar os serviçoes, execute o seguinte comando:
```sh
docker-compose up
```
ATENÇÃO: Este comando não finaliza e continua executando no terminal

## Acessando Airflow
Este é o link de acesso ao Airflow:  
http://localhost:8000

user: airflow  
senha: airflow

## Importando Dashboard no Grafana
Para importar um dashboard no Grafana, siga os passos abaixo:

1. Acesse o Grafana através do link: http://localhost:3000
2. Faça login com as credenciais:
    - Usuário: admin
    - Senha: admin
3. Clique no botão "New" e selecione "Import".
4. Faça o upload do arquivo "case_tecnico.json" localizado na pasta grafana\dashboards 
    Você pode importar um dashboard de duas maneiras:
        - Fazendo upload de um arquivo JSON.
        - Colando o JSON do dashboard diretamente no campo fornecido.
5. Após escolher a forma de importação, clique em "Import".

ATENÇÃO! - Se houver problema com a conexão com a fonte após a importação, selecione a edição do elemento e clique em "Run Query"