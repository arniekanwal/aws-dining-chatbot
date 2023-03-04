from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

host = 'https://search-diningsearch-hgalbkaccpydlhouymrgk3k23a.us-east-1.es.amazonaws.com'
region = 'us-east-1'  # e.g. us-west-1

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


def lambda_handler(event, context):
    print(event)

    for entry in event['Records']:
        if entry['eventName'] == 'INSERT' or entry['eventName'] == 'MODIFY':
            entry = entry['dynamodb']
            entryBody = {
                "cuisine": entry['NewImage']['cuisine']['S'],
                "id": entry['NewImage']['id']['S'],
            }
            index = "restaurants"
            type = "restaurant"
            if entryBody['id'] == 'test':
                index = "test"
                type = "test"
            response = es.index(index=index, doc_type=type, id=entryBody['id'], body=entryBody)
            print(response)
            if es.indices.exists(index="test"):
                es.indices.delete(index="test")