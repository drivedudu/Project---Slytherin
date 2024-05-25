import requests
import pandas as pd
from sqlalchemy import create_engine
# Create engine
import getpass  # To get the password without showing the input

def create_cnt():
    password = getpass.getpass()
    bd = "lab_potter"
    connection_string = 'mysql+pymysql://root:' + password + '@localhost/'+bd
    engine = create_engine(connection_string)
    return engine

def dropcount(data):
    data.drop(columns='count', inplace=True)
    return data

def countvalues(data, nameb):
    data = data[nameb].value_counts().reset_index()
    data.index = data.index + 1
    data = dropcount(data)
    return data

def save_to_sql(dataframes, engine):
    for name, df in dataframes.items():
        df.to_sql(name, con=engine, if_exists='replace', index=True)

def print_json_keys(data, prefix=""):
    """
    Função para imprimir as chaves disponíveis em um objeto JSON de forma hierárquica
    Argumentos:
    - data: Os dados em formato JSON
    - prefix: O prefixo para indicar a hierarquia das chaves (usado recursivamente)
    """
    if data:
        # check is a list of the dict
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            print(prefix + str(len(data)))
            print(prefix + "Lista:")
            print_json_keys(data[0], prefix + "  ")
        # check is a dict
        elif isinstance(data, dict):
            print(prefix + "Dicionário:")
            for key, value in data.items():
                print(prefix + "  -", key)
                if isinstance(value, (list, dict)):
                    print_json_keys(value, prefix + "    ")
        else:
            print("No suported")
    else:
        print(f'{prefix} {data}  - {type(data)}')
        # print("Nenhum dado disponível para análise")

# Exemplo de uso da função


def print_keys(data):
    """
    Função para imprimir as chaves disponíveis em um objeto JSON
    Argumentos:
    - data: Os dados em formato JSON
    """
    if data:
        # Verifica se é uma lista de dicionários
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            keys = data[0].keys()
            print("Chaves disponíveis:")
            for key in keys:
                print("-", key)
        # Verifica se é um dicionário
        elif isinstance(data, dict):
            keys = data.keys()
            print("Chaves disponíveis:")
            for key in keys:
                print("-", key)
        else:
            print("Os dados não estão em um formato suportado")
    else:
        print("Nenhum dado disponível para análise")



def create_dataframe(json_data):
    """
    Função para criar um DataFrame do pandas a partir de dados JSON e seus valores
    Argumentos:
    - json_data: Os dados em formato JSON
    Retorna:
    - Um DataFrame do pandas
    """
    if json_data:
        # Normaliza os dados JSON em um DataFrame
        df = pd.json_normalize(json_data)
        return df
    else:
        print("Nenhum dado disponível para criar o DataFrame")
        return None