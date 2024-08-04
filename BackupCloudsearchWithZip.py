import boto3
import json
import zipfile
from datetime import datetime, timedelta
import os
import tempfile

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
    
# Fonction pour formater la date en dossier
def format_date_to_ZipFile(date):
    return date.strftime("%Y-%m-%d")
    
# Fonction pour formater les dates pour la requête CloudSearch
def format_date_for_query_begin(date):
    return date.strftime("%Y-%m-%dT00:00:00Z")
    
def format_date_for_query_end(date):
    return date.strftime("%Y-%m-%dT23:59:59Z")
    
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
        # Création d'un fichier ZIP temporaire
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
            zip_path = temp_zip.name
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:
                next_token = None
                while True:
                    # Construire la requête pour CloudSearch
                    querydate = f"{field_date}:['{format_date_for_query_begin(current_date)}', '{format_date_for_query_end(next_date)}']"
                    query = {
                        'query': querydate,
                        'size': batch_size,
                        'queryParser': 'structured',
                        'cursor': next_token if next_token else 'initial'
                    }
                    
                    try:
                        response = cloudsearch.search(**query)
                        cur_token = next_token
                        next_token = response.get('hits', {}).get('cursor', None)

                        hits = response.get('hits', {}).get('hit', [])
                        total_hits = response.get('hits', {}).get('found', 0)

                        # Parcourir les hits et les sauvegarder dans S3
                        for hit in hits:
                            i = i+1
                            if i % 10 == 0 or i == total_hits:
                                print_progress_bar(i, total_hits, prefix=f'Progress {format_date_to_path(current_date)} :', suffix=f'{i}/{total_hits} Complete', length=100)           
                            document_id = hit['id']
                            document_json = json.dumps(hit['fields'])
                            zip_file.writestr(f"{document_id}.json", document_json)
                            
                            # Dossier basé sur la date
                            folder_path = format_date_to_path(current_date)
                            s3_key = os.path.join(folder_path, f"{document_id}.json")
                                             # Si aucun next_token n'est retourné, nous avons récupéré tous les résultats
                        if not next_token or cur_token == next_token:
                            #print(f"Aucun nextToken trouvé, fin de la pagination.{next_token}")
                            break
                    except Exception as e:
                        print(f"Erreur lors de la récupération des données : {e}")
                        break
        # Vérifier si le fichier ZIP est vide
        if total_hits > 0:
            # Téléverser le fichier ZIP vers S3
            zip_key = os.path.join(format_date_to_path(current_date), f'{format_date_to_ZipFile(current_date)}.zip')
            print(f"Lancement upload Fichier ZIP {zip_key}")
            retries = 0
            while retries < max_retries:
                try:
                    s3.upload_file(zip_path, bucket_name, zip_key)
                    print(f"Fichier ZIP {zip_key} téléversé avec succès.")
                    break
                except Exception as e:
                    retries += 1
                    print(f"Erreur lors de l'upload du fichier ZIP {zip_key} : {e}")
                    if retries == max_retries:
                        print(f"Échec de l'upload du fichier ZIP {zip_key} après {max_retries} tentatives.")
                        break

        else:
            print(f"Aucun document trouvé pour la date {current_date.strftime('%Y-%m-%d')}, fichier ZIP vide.")
        # Supprimer le fichier ZIP temporaire
        os.remove(zip_path)

        current_date = next_date

# Exécution de la fonction
sauvegarder_documents()
