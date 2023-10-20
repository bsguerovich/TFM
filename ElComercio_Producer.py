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
def enviar_noticias_reuters_a_event_hub(producer):
    url = 'https://elcomercio.pe/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Encontrar todos los elementos de noticias con la clase 'title'
    titulares_elems = soup.find_all('h2', class_='fs-wi__title')
    
    evento_nro = 0  # Inicializa la variable contador de eventos

    for titular_elem in titulares_elems:
        titular = titular_elem.text.strip()
        link = titular_elem.a['href'] if titular_elem.a else None
        link = 'https://elcomercio.pe/' + link
        empresa = 'El Comercio'
        
        # Convertimos la noticia a formato string (por ejemplo, JSON) para enviarla
        noticia = {
            'Empresa': empresa,
            'Titular': titular,
            'Link': link
        }
        event_data = EventData(json.dumps(noticia))
        
        # Se envía la noticia al Event Hub de manera individual.
        producer.send_batch([event_data])
        time.sleep(1)
        evento_nro += 1  # Incrementa el contador de eventos
        
        # Imprimimos la noticia y el número del evento justo después de enviarla
        print(f"Evento Nro {evento_nro}: Enviando noticia:", noticia)

producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)

contador = 0  # Inicializa la variable contador para ejecuciones

while True:
    contador += 1
    print(f"\nEjecucion numero {contador}")  # Se muestra primero el número de ejecución
    enviar_noticias_reuters_a_event_hub(producer)  # Enviar las noticias al Event Hub
    time.sleep(600)

producer.close()