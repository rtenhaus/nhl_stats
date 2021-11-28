import os
from google.cloud import bigquery



table_plays = 'rtenha.nhl_raw.plays'
table_shifts = 'rtenha.nhl_raw.shifts'
table_games = 'rtenha.nhl_raw.games'
table_shots = 'rtenha.nhl_raw.shots'

# Config BQ
bq_client = bigquery.Client()
# project_ref = bq_client.project('rtenha')
dataset_ref = bq_client.dataset('nhl_staging')

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

# All Extract files have the same format, game_id, game_date, data (stringified JSON)
schema = [
    bigquery.SchemaField('game_id', 'INT64', 'NULLABLE'),
    bigquery.SchemaField('game_date', 'DATE', 'NULLABLE'),
    bigquery.SchemaField('data', 'STRING', 'NULLABLE')  # BQ used to have a JSON column type, no longer
]

job_config.schema = schema


# Write the games table (only 1)
with open('../data/games/all_games.jsonl', 'rb') as source_file:
    job = bq_client.load_table_from_file(source_file, table_games, job_config = job_config)
    job.result()


# Write the plays to table
plays_dir = '../data/plays'

for file in os.listdir(plays_dir):
    with open(os.path.join(plays_dir,file), 'rb') as source_file:
        job = bq_client.load_table_from_file(source_file, table_plays, job_config = job_config)
        job.result()


# Write the shots to table
shots_dir = '../data/shots'

for file in os.listdir(shots_dir):
    with open(os.path.join(shots_dir,file), 'rb') as source_file:
        job = bq_client.load_table_from_file(source_file, table_shots, job_config = job_config)
        job.result()


# Write the shifts to table
shifts_dir = '../data/shifts'

for file in os.listdir(shifts_dir):
    with open(os.path.join(shifts_dir,file), 'rb') as source_file:
        job = bq_client.load_table_from_file(source_file, table_shifts, job_config = job_config)
        job.result()


print('Done')