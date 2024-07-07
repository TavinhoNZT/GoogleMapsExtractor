import googlemaps  # type: ignore
import pandas as pd  # type: ignore
import time
import requests  # type: ignore
from bs4 import BeautifulSoup  # type: ignore
import re
from textblob import TextBlob  # type: ignore
import logging
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Substitua pela sua chave de API do Google Maps usando uma variável de ambiente
API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', 'SUA_CHAVE_API_AQUI')

# Inicialize o cliente do Google Maps
gmaps = googlemaps.Client(key=API_KEY)

# Função para analisar sentimento das avaliações
def analyze_review_sentiment(reviews):
    sentiments = [TextBlob(review['text']).sentiment.polarity for review in reviews]
    return sum(sentiments) / len(sentiments) if sentiments else None


# Função para extrair dados de uma página de resultados
def extract_place_details(results, existing_place_ids):
    data = []
    for place in results:
        place_id = place['place_id']
        
        if place_id in existing_place_ids:
            continue
        
        place_details = gmaps.place(place_id=place_id)
        details = place_details['result']
        name = details.get('name')
        address = details.get('formatted_address')
        phone = details.get('formatted_phone_number')
        website = details.get('website')
        rating = details.get('rating')
        user_ratings_total = details.get('user_ratings_total')
        
        # Analisar sentimento das avaliações
        reviews = details.get('reviews', [])
        sentiment = analyze_review_sentiment(reviews)
        
        # Tenta extrair o e-mail do site, se disponível
        email = None
        if website:
            email = extract_email_from_website(website)
        
        data.append([name, address, phone, website, email, rating, user_ratings_total, sentiment])
        existing_place_ids.add(place_id)
        
        time.sleep(1)  # Espera para não exceder os limites de taxa da API
    
    return data

# Função para tentar extrair o e-mail de um site
def extract_email_from_website(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        email = re.search(r'[\w\.-]+@[\w\.-]+', soup.text)
        if email:
            return email.group(0)
    except Exception as e:
        logging.error(f"Erro ao acessar {url}: {e}")
    return None

# Função para realizar a pesquisa e extrair dados, evitando duplicatas
def get_places_data(query, max_results=200):
    places_result = gmaps.places(query=query)
    all_data = []
    existing_place_ids = set()

    all_data.extend(extract_place_details(places_result['results'], existing_place_ids))

    while 'next_page_token' in places_result and len(all_data) < max_results:
        next_page_token = places_result['next_page_token']
        time.sleep(5)  # Espera de 5 segundos antes de fazer uma nova requisição com next_page_token
        places_result = gmaps.places(query=query, page_token=next_page_token)
        all_data.extend(extract_place_details(places_result['results'], existing_place_ids))
    
    return all_data

# Realize pesquisas em diferentes regiões e combine os resultados
queries = ['petshop em São Paulo', 'petshop em Santo André', 'petshop em São Bernardo do Campo', 'petshop em São Caetano do Sul', 'petshop em Maúa']
combined_data = []

for query in queries:
    combined_data.extend(get_places_data(query, max_results=200))

# Criar um DataFrame do pandas
df = pd.DataFrame(combined_data, columns=['Nome', 'Endereço', 'Telefone', 'Site', 'E-mail', 'Avaliação', 'Quantidade de Avaliações', 'Sentimento'])

# Filtrar para incluir apenas empresas com telefone
df = df[df['Telefone'].notnull()]

# Exportar para um arquivo Excel
output_path = 'O_CAMINHO_DA_PASTA_AQUI'
df.to_excel(output_path, index=False)

logging.info("Dados exportados com sucesso!")
