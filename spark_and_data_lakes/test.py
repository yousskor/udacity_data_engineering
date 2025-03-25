

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Retrieve configuration values from the config file:
ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# SQL command to copy data from S3 to staging_events table
staging_events_copy = ("""
 COPY staging_events 
 FROM {}
 CREDENTIALS 'aws_iam_role={}'
 REGION 'us-east-2'   
 JSON {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

print(staging_events_copy)



staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-east-2'  
FORMAT AS JSON 'auto'
COMPUPDATE OFF;  # Ajout recommandé pour les chargements initiaux
""").format(SONG_DATA, ARN)

print(staging_songs_copy)

"""
""
"aws redshift create-cluster \
  --cluster-identifier mon-cluster-redshift \
  --node-type dc2.large \
  --master-username dwhuser \
  --master-user-password Passw0rd \
  --cluster-type multi-node \
  --number-of-nodes 2 \
  --db-name dwh \
  --iam-roles arn:aws:iam::221814860803:user/awsuser"
"""

