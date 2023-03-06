import logging
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_sqs_data():
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/401380617687/dining_paramq'
    
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'time', 'cuisine', 'location', 'num_people', 'phNo'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        messages = response['Messages'] if 'Messages' in response.keys() else []

        for message in messages:
            receiptHandle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receiptHandle)
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
        hosts=[{'host': host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    es_result=esClient.search(index="restaurants", body=query)    # response=es.get()
    return es_result
    
    
def get_dynamo_data(dynno, table, key):
    response = table.get_item(Key={'id':key}, TableName='yelp-restaurants')
    name = response['Item']['name']
    address_list = response['Item']['address']
    return '{}, {}'.format(name, address_list)

def lambda_handler(event, context):
    
    # Create SQS client
    sqs = boto3.client('sqs')

    es_host = 'https://search-diningsearch-hgalbkaccpydlhouymrgk3k23a.us-east-1.es.amazonaws.com' 
    table_name = 'yelp-restaurants'
    
    messages = get_sqs_data('https://sqs.us-east-1.amazonaws.com/401380617687/dining_paramq')
    
    logging.info(messages)
        
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    

    for message in messages:
        logging.info(message)
        msg_attributes=message['MessageAttributes']
        query = {"query": {"match": {"cuisine": msg_attributes["cuisine"]["StringValue"]}}}
        es_search_result = es_search(es_host, query)
        number_of_records_found = int(es_search_result["hits"]["total"]["value"])
        hits = es_search_result['hits']['hits']
        suggested_restaurants = []
        for hit in hits:
            id = hit['_source']['id']
            suggested_restaurant = get_dynamo_data(dynamodb, table, id)
            suggested_restaurants.append(suggested_restaurant)

        text = "Hello! Here are the "+msg_attributes['cuisine']['StringValue']+ " suggestions for "+msg_attributes['people']['StringValue']+" people at "+ msg_attributes['time']['StringValue']+" "
        for i,rest in enumerate(suggested_restaurants):
            text += "(" + str(i+1) + ")" + rest
        

        # phone_number = msg_attributes['phone_number']
        sns_client = boto3.client('sns', 'us-east-1')
        
        response = sns_client.publish(
            TopicArn="arn:aws:sns:us-east-1:401380617687:food_recommender.fifo", 
            Message=text
        )

        print(response, "completed")