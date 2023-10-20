# IMPORT
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import numpy as np
import json

# Parametros, conexion al Azure Event Hub
connection_str = "Endpoint=sb://web-eventhubs.servicebus.windows.net/;SharedAccessKeyName=my-sender;SharedAccessKey=oc0YKBsjbQiSZq8VtnDQPB7dj9O4gQVpl+AEhGZm3gU=;EntityPath=webscraping"
eventhub_name = "webscraping"

# WEB SCRAPING
def obtener_noticias_bbc():
    url = 'https://www.bbc.com/news'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    noticias = soup.find_all('div', class_='gs-c-promo')
    
    evento_nro = 0  # Inicializa la variable contador de eventos
    
    for noticia in noticias:
        titular_elem = noticia.find('h3', class_='gs-c-promo-heading__title')
        
        if titular_elem:
            titular = titular_elem.text.strip()
            link = titular_elem.parent['href']
            if not link.startswith(('http', 'https')):
                link = 'https://www.bbc.com' + link
                empresa = 'BBC'

            noticia_dict = {'Empresa': empresa, 'Titular': titular, 'Link': link}

            # Convertimos la noticia a formato string (por ejemplo, JSON) para enviarla
            event_data = EventData(json.dumps(noticia_dict))
            
            # Enviamos la noticia directamente a Event Hub
            producer.send_batch([event_data])
            time.sleep(1)
            evento_nro += 1  # Incrementa el contador de eventos

            print(f"Evento Nro {evento_nro}: Enviando noticia:", noticia_dict)

# Configuración de Event Hub
producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)

contador = 0  # Inicializa la variable contador

while True:
    contador += 1
    print(f"\nEjecucion numero {contador}")  # Se muestra primero el número de ejecución
    obtener_noticias_bbc()  # Directamente obtenemos y enviamos las noticias al Event Hub
    time.sleep(600)

producer.close()