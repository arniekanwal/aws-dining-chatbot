import boto3
import requests
import os
import json
from dotenv import load_dotenv
from decimal import Decimal

'''
Global Variables
'''
partition_key = "restaurant_id" 
cuisine_types = ["thai", "chinese", "mexican", "indian", "sushi", "italian", "pizza", "french", "ramen", "korean"]
yelp_url = "https://api.yelp.com/v3/businesses/search"


'''
Take the results of API query and add 
information entry by entry into DynamoDB table
'''
def add_to_table(table, metadata):
    response = table.put_item(
        Item=metadata
    )
    return response["ResponseMetadata"]["HTTPStatusCode"]

'''
This function makes a call to Yelp API
and queries data on restaurants by category
'''
def scrape_from_yelp(url, headers, cuisine_type, table):
    parameters = {
        'location': 'New York, NY',
        'term': 'restaurant',
        'categories': cuisine_type,
        'sort_by': 'best_match',
        'limit': 50,
    }

    # Query Yelp API and return JSON metadata as Python Dict
    r = requests.get(url=url, headers=headers, params=parameters).json()
    response = json.loads(json.dumps(r), parse_float=Decimal)
    
    # Parse data, append to DynamoDB, add indices to ElasticSearch
    for store in response['businesses']:
        metadata = {
            partition_key: store['id'],
            "Cuisine": store['categories'][0].get('alias', cuisine_type),
            "Name": store.get('name', 'null'),
            "Coordinates": store.get('coordinates', 'null'),
            "Number of Reviews": Decimal(store.get('review_count', 0)),
            "Rating": Decimal(store.get('rating', 0)),
            "Address": store['location'].get('address1', 'null'),
            "Zip Code": store['location'].get('zip_code', 'null'),
        }
        add_to_table(table, metadata)

    return response


if __name__ == "__main__":
    # Parse local .env file for API keys
    load_dotenv(dotenv_path="./.env.local")
    headers = {
        "accept": "application/json",
        "Authorization": 'Bearer %s' % os.getenv('YELP-API-KEY')
    }

    # Initialize the DynamoDB Table
    table = boto3.resource('dynamodb').Table('yelp-restaurants')

    # Iterate through cuisines and scrape restaurants...
    # Their values will be added to DynamoDB Table and OpenSearch Instance
    for cuisine in cuisine_types:
        scrape_from_yelp(yelp_url, headers, cuisine, table)

    print("\n=============\nFinished Scraping!!\n=============\n")


