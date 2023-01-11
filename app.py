import csv
import requests
import os
import uuid
import pandas as pd

# Importamos las librerías de Azure
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient

# Establecemos las URLs de los endpoints
posts_url = "https://test.com/wp-json/wp/v2/posts"
categories_url = "https://test.com/wp-json/wp/v2/categories"
tags_url = "https://test.com/wp-json/wp/v2/tags"

# Realizamos las solicitudes a los endpoints y obtenemos las respuestas
posts_response = requests.get(posts_url)
categories_response = requests.get(categories_url)
tags_response = requests.get(tags_url)

# Verificamos que las respuestas sean exitosas
if posts_response.status_code == 200 and categories_response.status_code == 200 and tags_response.status_code == 200:
  # Si las respuestas son exitosas, obtenemos la información de los endpoints
  posts = posts_response.json()
  categories = categories_response.json()
  tags = tags_response.json()

  # Creamos un archivo CSV para escribir los datos
  with open('results.csv', 'w', newline='', encoding='utf-8') as csvfile:
    # Creamos el objeto de escritura de CSV
    writer = csv.writer(csvfile)

    # Escribimos los encabezados de las columnas
    writer.writerow(['Titulo', 'Autor', 'Categorias', 'Tags', 'Fecha'])

    # Iteramos sobre los resultados de cada endpoint
    for post, category, tag in zip(posts, categories, tags):
      # Escribimos la información en el archivo CSV
      writer.writerow([post['title']['rendered'], post['yoast_head_json']['author'], category['name'], tag['name'], post['date']])

  # Nombre del contenedor donde se guardará el archivo
  container_name = "metricas"

  # Dirección URL de la cuenta de almacenamiento de Azure
  AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net"

  # Usamos la cadena de conexión para crear un cliente de servicio de blob
  blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

  # Verificamos si el contenedor existe
  if container_name not in [c.name for c in blob_service_client.list_containers()]:
      # Si el contenedor no existe, lo creamos
      container_client = blob_service_client.create_container(container_name)
  else:
    # Si el contenedor ya existe, obtenemos un cliente para trabajar con él
    # Creamos un cliente de contenedor
    container_client = blob_service_client.get_container_client(container_name)
  unique_file_name = str(uuid.uuid1()) + "_" + 'results.csv'
  # df = pd.read_csv('results.csv', encoding='utf-8')
  # data = df.to_csv(index=False)
  # with BlobClient.from_connection_string(conn_str=AZURE_STORAGE_CONNECTION_STRING, container_name=container_name, blob_name=unique_file_name) as blob:
  #   blob.upload_blob(data, overwrite=True)   

  # Generamos un nombre de archivo único para evitar colisiones
  unique_file_name = str(uuid.uuid1()) + "_" + 'results.csv'

  # Creamos un cliente de blob para el archivo a subir
  blob_client = container_client.get_blob_client(unique_file_name)

  # Abrimos el archivo en modo lectura binaria
  with open('results.csv', "rb") as data:
      #file_content = data.read()

      # Subimos el archivo al blob storage
      blob_client.upload_blob(data)

# Si alguna de las respuestas no fue exitosa, mostramos un mensaje de error
else:
  print("Error al obtener la información de los endpoints")

# # Eliminamos el archivo CSV temporal
# os.remove('results.csv')
