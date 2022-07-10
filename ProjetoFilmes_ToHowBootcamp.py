#O objetivo desse notebook é concluir a primeira etapa do Projeto descrito no documento "Proposta do Projeto.docx" contido nessa mesma pasta do GIT 
#Toda a primeira etapa de leitura de API, tranformações e carga no s3 está contida nesse notebook


#Extração de Pacotes necessários para executar o código Python
from datetime import date
import config # to hide TMDB API keys
import requests # to make TMDB API calls
import pandas as pd
from datetime import date, timedelta
from ast import literal_eval
import numpy as np
import boto3
import os
import posixpath
from botocore.exceptions import ClientError
import s3fs
from io import StringIO


##API Key do site https://www.themoviedb.org/settings/api
api_key = [sua api key]


#15 dias para trás
two_week = date.today() - timedelta(days=15)
two_week = two_week.strftime('%Y-%m-%d')
two_week

#Fazendo a requisição na API
response = requests.get('https://api.themoviedb.org/3/discover/movie?api_key=' +  api_key + '&primary_release_date.gte='+ two_week + '&vote_count.gte=50' +'&sort_by=vote_average.desc'+'&language=pt-BR')
response

#Salvando os dados que a API retorna
api_filmes = response.json()
api_filmes = api_filmes['results']

#Separando quais colunas vamos utilizar e criando o dataframe pandas onde a lista de filmes será 
#armazenada
columns = ['film','genres','vote_average','vote_count','release_date']
df_api_filmes = pd.DataFrame(columns=columns)


#For para trazer a lista de filmes e carregar as colunas já previamente selecionadas
for film in api_filmes:

    film_list = requests.get('https://api.themoviedb.org/3/movie/'+ str(film['id']) +'?api_key='+ api_key+'&language=pt-BR')
    film_list = film_list.json()

    df_api_filmes.loc[len(df_api_filmes)]=[film['title'],film_list['genres'], film_list['vote_average'], film_list['vote_count'],film_list['release_date']]

#Consultando os filmes carregados
df_api_filmes.head(100)


#Criando dataframes auxiliares para começar a transformação dos dados
df_api_filmes_aux = df_api_filmes
df_api_filmes_aux2 = df_api_filmes_aux.join(pd.DataFrame(df_api_filmes_aux.pop('genres').values.tolist()))


#A principal transformação é no campo genre porque vem com o formato .json
df_api_filmes_aux2.rename(columns = {0:'GEN0',1:'GEN1', 2:'GEN2', 3:'GEN3'}, inplace = True)


#GEN0
df_api_filmes_aux2 = df_api_filmes_aux2.join(pd.json_normalize(df_api_filmes_aux2.GEN0))
df_api_filmes_aux2.drop(columns=['GEN0', 'id'], inplace=True)
df_api_filmes_aux2.rename(columns = {'name':'genre_1'}, inplace = True)

#GEN1
df_api_filmes_aux2 = df_api_filmes_aux2.join(pd.json_normalize(df_api_filmes_aux2.GEN1))
df_api_filmes_aux2.drop(columns=['GEN1', 'id'], inplace=True)
df_api_filmes_aux2.rename(columns = {'name':'genre_2'}, inplace = True)


#GEN2
df_api_filmes_aux2 = df_api_filmes_aux2.join(pd.json_normalize(df_api_filmes_aux2.GEN2))
df_api_filmes_aux2.drop(columns=['GEN2', 'id'], inplace=True)
df_api_filmes_aux2.rename(columns = {'name':'genre_3'}, inplace = True)


#GEN3
df_api_filmes_aux2 = df_api_filmes_aux2.join(pd.json_normalize(df_api_filmes_aux2.GEN3))
df_api_filmes_aux2.drop(columns=['GEN3', 'id'], inplace=True)
df_api_filmes_aux2.rename(columns = {'name':'genre_4'}, inplace = True)
df_api_filmes_aux2


#Finalizada as transformações, vamos separar quais colunas serão levadas para o s3 e adicionar uma data controle
df_final = df_api_filmes_aux2[['film', 'genre_1', 'genre_2','vote_average','vote_count','release_date']]
df_final['date_begin_twoweek'] = two_week

df_final.head(100)



#Preparando para subir no s3
data = date.today()
datastr = str(data)[:10]
datastr = datastr.replace('-', '')
datastr


#Subindo no s3
s3 = boto3.client("s3",\
                  region_name=[sua region name],\
                  aws_access_key_id=[sua access key],\
                  aws_secret_access_key=[sua secret access key])
csv_buf = StringIO()
df_final.to_csv(csv_buf, index = False, sep=';', encoding='UTF-8')
csv_buf.seek(0)
s3.put_object(Bucket=[seu bucket], Body=csv_buf.getvalue(), Key=[sua pasta]+str(datastr)+'_ListaFilmes15dias.csv')