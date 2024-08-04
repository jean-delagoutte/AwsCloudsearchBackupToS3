# AwsCloudsearchBackupToS3
Script python permettant de sauvegarder un index cloudsearch  sur un bucket S3

# Principe de fonctionnement
Sur une plage de date, le script :
- parcourt chaque date de la plage 
- selectionne un ensemble de document sur l'index Cloudsearch ayant un champs date avec cette date
- Script BackupCloudSearch : créé une arborescence YEAR\MONTH\DAY\ sur le bucket et y enregistre le document avec son_id.json
- Script BackupCloudSearchWithZip : créé une arborescence YEAR\MONTH\DAY\ et cré un zip YEAR-MONTH-DAY.zip contenant tous les documents puis le push sur le S3

nb : 
BackupCloudSearch ne fonctionne pas sur les gros volume >10k document. Pour contrer ca il faudrait adapter pour utliser la méthode avec les curseur présente dans BackupCloudsearchWithZip
BackupCloudsearchWithZip est fonctionnel

# Fonctionnement :
Remplir la config et la plage de date avec ces propres informations 
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

# execution duc script 
python.exe BackupCloudsearchWithZip.py
