import logging
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_sqs_data():
    sqs = boto3.client('sqs')
    queue_url = f'https://sqs.us-east-1.amazonaws.com/{user_id}/dining_paramq.fifo'
    
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'dining_time', 'cuisine', 'location', 'people', 'phone_number', 'dining_date'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        messages = response['Messages'] if 'Messages' in response.keys() else []
        return messages
    
    except ClientError as e:
        logging.error(e)
        return []
        

def es_search(host, query):
    credentials = boto3.Session().get_credentials()

    region = 'us-east-1'  # e.g. us-west-1
    service = 'es'

    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    esClient = Elasticsearch(
        hosts=[{'host': host, 'port': 443, 'scheme': "https"}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    es_result=esClient.search(index="restaurants", body=query)    # response=es.get()
    return es_result
    
    
def get_dynamo_data(client, key):
    response = client.get_item(
        TableName="yelp-restaurants",
        Key={'restaurant_id': {'S': key}}
    )
    name = response['Item']['Name']
    address_list = response['Item']['Address']
    return '{}, {}'.format(name, address_list)

def lambda_handler(event, context):
    es_host = 'search-diningsearch-hgalbkaccpydlhouymrgk3k23a.us-east-1.es.amazonaws.com' 
    
    messages = get_sqs_data()
    logging.info(messages)
        
    dynamodb_client = boto3.client('dynamodb', region_name="us-east-1")

    for message in messages:
        logging.info(message)
        msg_attributes=message['MessageAttributes']
        query = {"query": {"match": {"cuisine": msg_attributes["cuisine"]["StringValue"].lower()}}}
        es_search_result = es_search(es_host, query)
        number_of_records_found = int(es_search_result["hits"]["total"]["value"])
        hits = es_search_result['hits']['hits']
        suggested_restaurants = []
        for hit in hits:
            id = hit['_id']
            suggested_restaurant = get_dynamo_data(dynamodb_client, id)
            suggested_restaurants.append(suggested_restaurant)

        text = "Hello! Here are the "+msg_attributes['cuisine']['StringValue']+ " suggestions for "+msg_attributes['people']['StringValue']+" people at "+ msg_attributes['dining_time']['StringValue']+" "
        for i,rest in enumerate(suggested_restaurants):
            text += "(" + str(i+1) + ")" + rest
        

        phone_number = "+1" + msg_attributes['phone_number']['StringValue']
        sns_client = boto3.client('sns', 'us-east-1')
        
        response = sns_client.publish(
            PhoneNumber=phone_number,
            Message=text
        )
        
        print(text)
        print(response, "completed")