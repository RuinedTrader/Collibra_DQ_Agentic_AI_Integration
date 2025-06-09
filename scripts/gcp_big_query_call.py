from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'../keys/gcp-collibra-integration-api-key.json'
client = bigquery.Client()

print(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

# Perform a query.
def run_query(query):
    """Runs the provided query in GCP Bigquery instance"""
    query_job = client.query(query)  # API request
    row = query_job.result()  # Waits for query to finish
    return row