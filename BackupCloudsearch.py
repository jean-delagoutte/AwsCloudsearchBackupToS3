import boto3
import json
from datetime import datetime, timedelta
import os

# Configuration
cloudsearch_domain = 'Cloudsearch-Domain'  # Nom du domaine CloudSearch
bucket_name = 'Bucket-Name'  # Nom du bucket S3
region_name = 'Region-Name'  # Exemple : 'us-west-2'
batch_size = 10000 # Nombre de documents à récupérer par lot
max_retries = 3  # Nombre maximum de tentatives en cas d'échec
field_date = 'field_date' # champs sur le quel la sauvegarde se fera
profile_name = 'Profile-Name' # Nom du profile configuré sur AwsCli

# Dates de début et de fin
date_begin = datetime(2021, 1, 1)
date_end = datetime(2021, 12, 31)

# Initialisation de la session AWS avec le profil
session = boto3.Session(profile_name=f'{profile_name}', region_name=f'{region_name}')

# Construction de l'URL d'endpoint CloudSearch
endpoint_url = f'https://search-{cloudsearch_domain}.{region_name}.cloudsearch.amazonaws.com'

# Initialisation des clients AWS avec la session
cloudsearch = session.client('cloudsearchdomain',endpoint_url=endpoint_url)

s3 = session.client('s3')

# Fonction pour formater la date en dossier
def format_date_to_path(date):
    return date.strftime("%Y/%m/%d/")
    
# Fonction pour formater les dates pour la requête CloudSearch
def format_date_for_query(date):
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
def print_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█', print_end="\r"):
    """
    Appelle cette fonction dans une boucle pour créer une barre de progression
    @param iteration   - Requis  : itération actuelle (int)
    @param total       - Requis  : itération totale (int)
    @param prefix      - Optionnel : préfixe à afficher (str)
    @param suffix      - Optionnel : suffixe à afficher (str)
    @param decimals    - Optionnel : nombre de décimales à afficher (int)
    @param length      - Optionnel : longueur de la barre de progression (int)
    @param fill        - Optionnel : caractère de remplissage de la barre (str)
    @param print_end   - Optionnel : caractère de fin d'affichage (str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    # Print New Line on Complete
    if iteration == total: 
        print()

# Fonction pour sauvegarder les documents
def sauvegarder_documents():
    current_date = date_begin

    while current_date <= date_end:
        next_date = current_date + timedelta(days=1)
        start = 0
        total_hits = 1  # Valeur initiale, mise à jour avec le nombre total de hits
        print_progress_bar(0, total_hits, prefix='Progress:', suffix='Complete', length=100)
        i = 0
        while start < total_hits:
            # Construire la requête pour CloudSearch
            query = f"{field_date}:['{format_date_for_query_begin(current_date)}', '{format_date_for_query_end(next_date)}']"
            try:
                response = cloudsearch.search(
                    query=query,
                    queryParser='structured',
                    start=start,
                    size=batch_size
                )
            except Exception as e:
                print(f"Erreur lors de la récupération des données : {e}")
                break

            hits = response.get('hits', {}).get('hit', [])
            total_hits = response.get('hits', {}).get('found', 0)

            # Parcourir les hits et les sauvegarder dans S3
            for hit in hits:
                if i % 10 == 0:
                    print_progress_bar(i, total_hits, prefix=f'Progress {format_date_to_path(current_date)} :', suffix=f'{i}/{total_hits} Complete', length=100)
                i = i+1
                document_id = hit['id']
                document = json.dumps(hit['fields'])

                # Dossier basé sur la date
                folder_path = format_date_to_path(current_date)
                s3_key = os.path.join(folder_path, f"{document_id}.json")

                # Upload to S3 avec gestion des tentatives
                retries = 0
                while retries < max_retries:
                    try:
                        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=document)
                        break
                    except Exception as e:
                        retries += 1
                        print(f"Erreur lors de l'upload de {document_id} : {e}")
                        if retries == max_retries:
                            print(f"Échec de l'upload de {document_id} après {max_retries} tentatives.")
                            break

            start += batch_size

        current_date = next_date

# Exécution de la fonction
sauvegarder_documents()
