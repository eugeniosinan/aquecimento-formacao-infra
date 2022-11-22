## Importando as bibliotecas
import requests
import pandas as pd
import json
from pandas import json_normalize
import numpy as np
# import seaborn as sns
import os
import sys
import urllib.parse

from io import StringIO
from prefect import task
from utils import log

@task
def get_api_data( rows=500, gender="", nat="" ):
    """Função para baixar informações da API Random User (https://randomuser.me/)
 
    Args:
        rows (int): Número de registros baixados
            default: 1000
            options: Number between 1 and 5000
            example: 2000            


        gender (str): Gênero do usuário.
            default: ""
            options: female / male
            example: female           


        nat (str): Naturalidade do usuário. Pode adicionar mais de uma naturalidade separada por vírgula
            default: ""
            options: AU, BR, CA, CH, DE, DK, ES, FI, FR, GB, IE, IN, IR, MX, NL, NO, NZ, RS, TR, UA, US
            example: BR,US            
    
    Return: Dataframe 

    """

    ## Validacao de parametros
    try:
        if 1 <= rows <= 5000:

            ## Setando o endpoint
            endpoint = f"https://randomuser.me/api/?results={rows}&"

            parameters_api =  []
            if gender != "":
                g = f"gender='{gender}'"
                parameters_api.append( g )

            if nat != "":
                n = f"nat='{nat}'"
                parameters_api.append( n )

            
            parameters_api = "&".join(parameters_api)
            print(parameters_api)
            
            if parameters_api == "":  
                ## Montando a query URL
                query = endpoint.rstrip('&') 
                print( query )
            else:
                query = endpoint + parameters_api
                print( query)

            r = requests.get( query )

            # Tranformar o objeto r em uma string json 
            dict= json.loads(r.text)

            # Normalizar a string json, pois existem objetos aninhados. Estes receberam o separador "_" 
            df = json_normalize( dict['results'], sep = "_" )

            log("Dados baixados, tratados e transformados em Dataframe...")

            return df

        else:
            log("Quantidade de linhas maior ou menor que a permitida ( 1 a 5000")
            sys.exit()            
                    
    except Exception as error:
        log ("Exception TYPE:", type(error))
        sys.exit()

@task
def create_partitions_country_state( df ):
    ## Função para criar as pastas por país e cidade

    try:

        ## Pegando o caminho da pasta do script
        path_script = os.path.dirname(os.path.realpath("__file__"))

        # Criar se nao existir o diretorio
        if not os.path.exists(os.path.join( path_script,'partitions')):
            os.makedirs(os.path.join( path_script,'partitions')) 

        path_partitions = os.path.join( path_script,'partitions')

        ## Criado um df com agrupamento de cidade e pais com o index resetado para não conter index multiplo
        df2 = df.groupby(by= [ 'location_country','location_state' ], dropna=False).count()[['gender']].rename( columns={'gender':'total'} ).reset_index()

        ## Criar dicionário com cidade na chave e país no valor
        dicionario_cidade_capital = pd.Series(df2.location_country.values,index=df2.location_state).to_dict()

        ## iterar neste dicionario para criar os bancos a partir das chaves e valores do dicionario contendo o parâmetro de seleçao da consulta ao df principal
        for key, value in dicionario_cidade_capital.items():
            
            pais = value
            cidade = key
            filename = f"{pais}_{cidade}.csv" 

            path_pais = os.path.join( path_partitions,"country=" + pais )
            path_cidade = os.path.join( path_partitions, "country=" + pais, "state=" + cidade )

            
            if not os.path.exists( path_pais ):     ## Criar se nao existir o diretorio de país
                os.makedirs( path_pais )
            ## 
            if not os.path.exists( path_cidade ):   ## riar se nao existir o diretorio de cidades
                os.makedirs( path_cidade )

            ## Criar os CSV's dentro das pastas
            df.loc[ ( df.location_country == pais ) & ( df.location_state == cidade )].to_csv( path_cidade + "//" +filename )

            log("Arquivos CSV gerados com sucesso!")

    except Exception as error:
        log ("Erro ao criar as partições", type(error))
        sys.exit()    

# ## Extraindo as informações com os parâmetros
# df_final = get_api_data( rows=10, gender="female", nat="US,BR" )   
# # print(df_final)
# create_partitions_country_state( df_final )



