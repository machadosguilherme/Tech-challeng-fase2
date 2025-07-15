import base64
import json
import requests
import pandas as pd
from datetime import date

def generate_historical_json_url(target_date):
    """
    Gera a URL para buscar a carteira em JSON de uma data específica (histórico).
    Esta é a única rota que respeita a data.
    """
    base_url = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDayByDate/"

    params = {
        "language": "pt-br",
        "pageNumber": 1,
        "pageSize": 150, # Suficiente para buscar todos os ativos do IBOV
        "index": "IBOV",
        "segment": "1",
        "refDate": target_date.strftime('%Y-%m-%d')
    }
    
    json_params = json.dumps(params, separators=(',', ':'))
    base64_params = base64.b64encode(json_params.encode('utf-8')).decode('utf-8')
    return f"{base_url}{base64_params}"

def fetch_and_save_as_csv(url, save_path):
    """
    Busca os dados JSON da URL, converte para um DataFrame Pandas e salva como CSV.
    """
    print(f"Buscando dados da URL: {url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        res = requests.get(url, headers=headers, timeout=20)

        if res.status_code == 200:
            data = res.json()
            
            if 'results' in data and data['results']:
                print(f"Sucesso! Dados recebidos para a data: {data['header']['date']}")
                
                # 1. Converter a lista 'results' do JSON em um DataFrame
                df = pd.DataFrame(data['results'])
                
                # 2. Limpar e transformar os dados
                df['data_referencia'] = pd.to_datetime(data['header']['date'], format='%d/%m/%y').date
                
                df.rename(columns={
                    'cod': 'ticker',
                    'asset': 'nome_ativo',
                    'type': 'tipo_ativo',
                    'part': 'participacao_percentual',
                    'theoricalQty': 'quantidade_teorica'
                }, inplace=True)
                
                # Conversão de tipos e limpeza de strings
                df['tipo_ativo'] = df['tipo_ativo'].str.strip()
                df['quantidade_teorica'] = df['quantidade_teorica'].str.replace('.', '', regex=False).astype('int64')
                df['participacao_percentual'] = df['participacao_percentual'].str.replace(',', '.', regex=False).astype('float64')
                
                # 3. Reordenar colunas para o formato final
                df_final = df[[
                    'data_referencia',
                    'ticker',
                    'nome_ativo',
                    'tipo_ativo',
                    'quantidade_teorica',
                    'participacao_percentual'
                ]]

                # 4. Salvar o DataFrame limpo em um arquivo CSV
                df_final.to_csv(
                    save_path,
                    sep=';',           # Separador ponto e vírgula, comum no Brasil
                    index=False,       # Não salvar o índice do DataFrame no arquivo
                    encoding='utf-8-sig' # Codificação que funciona bem com acentos e no Excel
                )
                print(f"Arquivo CSV salvo com sucesso em: {save_path}")
                print("\nAmostra dos dados salvos:")
                print(df_final.head())

            else:
                print(f"FALHA: A API respondeu, mas não retornou resultados. Pode ser um feriado ou dia sem pregão.")
        else:
            print(f"FALHA: A API retornou um erro. Status: {res.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão: {e}")

# --- EXECUÇÃO ---

# Escolha uma data passada que com certeza foi um dia útil
target_day = date(2025, 7, 1) # Sexta-feira, 11 de Julho de 2025

# Gera a URL correta (para o endpoint JSON)
historical_url = generate_historical_json_url(target_day)

# Define o nome do arquivo CSV de saída
output_csv_file = f"IBOV_carteira_{target_day.strftime('%Y-%m-%d')}.csv"

# Executa o processo completo
fetch_and_save_as_csv(historical_url, output_csv_file)