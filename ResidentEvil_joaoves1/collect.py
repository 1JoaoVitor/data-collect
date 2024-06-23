# %%
import requests 
from bs4 import BeautifulSoup
from tqdm import tqdm # biblioteca para ver quando tempo demora algumas ações
import pandas as pd

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'cookie': '_gid=GA1.2.1364550788.1719161359; __gads=ID=9a005c55ccf924fc:T=1719161358:RT=1719171830:S=ALNI_MZgZTTl8AJJdCSv-fxkOPPBV9JDzQ; __gpi=UID=00000db1b7d40023:T=1719161358:RT=1719171830:S=ALNI_MY05vpzw2ClGr3ZEkhVBu1xjYs8Ig; __eoi=ID=ad098e68829cb9f6:T=1719161358:RT=1719171830:S=AA-AfjYJ9dIQ-e7qQzROkLv7TBrp; _gat_gtag_UA_29446588_1=1; _ga=GA1.2.220782286.1719161358; FCNEC=%5B%5B%22AKsRol9wz6hR1L70ihKUrfUiMxHC5EF9dWbnZYqUtRdJJ9xis-N75_WyQTl_I_xnbqmeHWM4ECjLpK1qO3X5OGfGc6gAjHa1h5HbUlQQIRAAdDJ7GBb25CwzqKATAW9JuAznGxHxZUGnAIfP3o8SxCPUvJv5-u5eeg%3D%3D%22%5D%5D; _ga_DJLCSW50SC=GS1.1.1719171830.2.1.1719172056.56.0.0; _ga_D6NF5QC4QT=GS1.1.1719171830.2.1.1719172056.56.0.0',
    'referer': 'https://www.residentevildatabase.com/personagens/',
    'sec-ch-ua': '"Opera GX";v="109", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0',
}
# headers foram colocados para fora para serem usados em outras funções

def get_content(url):

    response = requests.get(url, headers=headers)
    return response

# esse headers é pego indo no site, abrindo o devtools (inspecionar)
# pegar o primeiro item quando aperta ctrl r e coiar o cUrl, jogar um site para 
# tranformar esse cUrl em código 


def get_basic_infos(soup):
    
    div_page = soup.find("div", class_="td-page-content")
    # busca uma div que tem como class "td-page-content"
    paragrafo = div_page.find_all("p")[1]
    ems = paragrafo.find_all("em")
    data = {}
    for i in ems:
        chave, valor, *_ = i.text.split(":") # *_ é usado para descompactar a lista "ignorando" o restante dos valores, usado pq deu problema em um dos links
        chave = chave.strip(" ") # remove os espaços no começo e final 
        data[chave] = valor.strip(" ") # remove os espaços
    # adicionando num dic as infos, separando por ":" e atribuindo chave e valor
    return data

def get_aparicoes(soup):
    lis = (soup
        .find("div", class_="td-page-content")
        .find("h4")
        .find_next()
        .find_all("li")
        )
    #passa o próximo h4 que tem e depois pega todos os li
    aparicoes = [i.text for i in lis]
    return aparicoes

def get_personagens_info(url):
    response = get_content(url)
    if response.status_code != 200:
        print("Não foi possível obter a página")
        return{} # vazio pq deu errado
    else:
        soup = BeautifulSoup(response.text)
        # pega o html devolvido pela resposta e 
        # transformou numa estrutura que podemos navegar
        data = get_basic_infos(soup)
        data["Aparicoes"] = get_aparicoes(soup)
    return data

def get_links(): 
    url = "https://www.residentevildatabase.com/personagens/"

    resp = requests.get(url, headers=headers)

    ancoras = (BeautifulSoup(resp.text)
                        .find("div", class_="td-page-content")
                        .find_all("a"))

    links = [i["href"] for i in ancoras] # função para pegar todos os links a partir da estrutura <a>.....</a>
    return links

# %%

links = get_links()
data = []
for i in tqdm(links):
    d = get_personagens_info(i)
    d["link"] = i
    data.append(d)

# %%

df = pd.DataFrame(data) # apareceu uma coluna com "de nascimento", provavelmente algum eror
df
# %%

# vamos achar esse erro 
condicao = ~df["de nascimento"].isna() 
# -true para nulo e false para não nulo, porém, com o ~ no começo a função faz a troca de valores 
df[condicao]
#achamos quem ta dando problema
# %%

df.to_parquet("dados_RE.parquet", index= False)

# nesse caso é melhor a transformação em parquet, mais adequado
# %%
