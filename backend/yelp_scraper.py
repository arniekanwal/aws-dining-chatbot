import boto3
import time
import requests
import os
from dotenv import load_dotenv

'''
Global Variables
'''
table_name = "yelp-restaurants"
partition_key = "insertedAtTimestamp"    
cuisine_types = ["Thai", "Chinese", "Mexican", "Indian", "Sushi", "Italian", "American", "Fast Food", "Ramen", "Korean"]
yelp_url = "https://api.yelp.com/v3/businesses/search"


'''
This function makes a call to Yelp API
and queries data on restaurants by category
'''
def yelp_query():
    pass

'''
Take the results of API query and add 
information entry by entry into DynamoDB table
'''
def modify_table():
    pass

if __name__ == "__main__":
    # Get local environment variables (i.e hidden keys) and initialize yelp_header
    load_dotenv()
    headers = {
        "Authorization": 'Bearer %s' % os.getenv('YELP-API-KEY')
    }

    # initialize the table
    DB = boto3.resource('dynamodb')
    table = DB.Table(table_name)

    # We will calculate the current time in milliseconds
    # and use that as the partition key

    curr_time = str(time.time() * 1000)



'''
    RTUxSyO5TKfUfjukmkEF5g

API Key
uHhiJ4Je9XyOwlHgvtR2ssFmht9a8T3HhDKlP16JNrqIAam6fiAQWjQZh_Qp7H6j7WUigOl2SKboypUnhBbOmaKb3xJrBM89W8xP9heLPwNUqktkN3sYZ0AxlFsBZHYx
'''

