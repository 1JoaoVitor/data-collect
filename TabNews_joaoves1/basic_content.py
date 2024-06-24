# %% 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import json
import time

# %%
def get_response(**kwargs): 
# o que é kwargs? 
# resposta: https://medium.com/rafaeltardivo/python-entendendo-o-uso-de-args-e-kwargs-em-funções-e-métodos-c8c2810e9dc8
    url = "https://www.tabnews.com.br/api/v1/contents/"
    resp = requests.get(url, params=kwargs)
    return resp

def save_data(data, option="json"):

    now = datetime.datetime.now().strftime("%Y-%m-%d-%Hh-%Mm-%Ss.%f")

    if option == "json":
        with open(f"data/contents/json/{now}.json", "w") as open_file: #with grante o fechamento do arquivo
            json.dump(data, open_file, indent=4)
    elif option == 'dataframe':
        df = pd.DataFrame(data)
        df.to_parquet(f"data/contents/parquet/{now}.parquet", index = False)



# %% 

page = 1
date_stop = pd.to_datetime('2024-01-01').date() # data limite que escolhemos
while True:
    print(page)
    resp = get_response(page=page, per_page=100, strategy="new") # esses parâmetros são dessa API em específica, estão no site de docs da API 
    if resp.status_code == 200:
        data = resp.json()
        save_data(data)

        date = pd.to_datetime(data[-1]["updated_at"]).date() # pegando a data da atualização do ultimo arquivo (pra esses arquivos em especifico)
        if len(data) < 100 or date < date_stop:
            break # ja pegou todos os arquivos 

        page += 1
        time.sleep(2)
    else:
        print(resp.status_code)
        print(resp.json())
        time.sleep(60 * 15)
# %%
