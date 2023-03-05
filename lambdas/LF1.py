import time
import os
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']
    

"""
Pass SQS Queue and message.
Returns dictionary of metadata on sent message
"""
def push_to_sqs(QueueURL, msg_body):    
    sqs = boto3.client('sqs')

    try:
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl = QueueURL,
            DelaySeconds = 0,
            MessageAttributes = {
                'cuisine': {
                    'DataType': 'String',
                    'StringValue': msg_body['cuisine']
                },
                'location': {
                    'DataType': 'String',
                    'StringValue': msg_body['location']
                },
                'phone_number': {
                    'DataType': 'Number',
                    'StringValue': msg_body['phone_number']
                },
                'dining_time': {
                    'DataType': 'String',
                    'StringValue': msg_body['dining_time']
                },
                'people': {
                    'DataType': 'Number',
                    'StringValue': msg_body['people']
                },
                'dining_date': {
                    'DataType': 'String',
                    'StringValue': msg_body['dining_date']
                }
            },
            MessageBody=(
                'Information regarding dining cuisine'
            )
        )
    
    except ClientError as e:
        logging.error(e) 
        return None
    
    return response
        

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
    
def build_validation_result(is_valid, violated_slot, message_content):
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def validate_parameters(_time_, _date_, cuisine_type, location, num_people, phone_number):
    
    location_types = ['manhattan', 'new york', 'ny', 'nyc']
    if not location:
        return build_validation_result(False, 'location', 'What city are you located in?')
    
    elif location.lower() not in location_types:
        return build_validation_result(False, 'location', 'I currently do not support recommendations for that location.')
    
    
    cuisine_types = ["thai", "chinese", "mexican", "indian", "sushi", "italian", "pizza", "french", "ramen", "korean"]
    if not cuisine_type:
        return build_validation_result(False, 'cuisine', 'What type of cuisine do you prefer to have?')
        
    elif cuisine_type.lower() not in cuisine_types:
        return build_validation_result(False, 'cuisine', 'I currently don\'t support {} recommendations, would you like to try a different cuisine'.format(cuisine_type))
    
    if not _date_:
        return build_validation_result(False, 'dining_date', 'What day are you looking to dine?')
    
    if not _time_:
        return build_validation_result(False, 'dining_time', 'What time do you prefer?')
    
    if not num_people:
        return build_validation_result(False, 'people', 'How many people would you like to reserve for?')
    
    if not phone_number:
        return build_validation_result(False, 'phone_number', 'Please share your phone number')
    
    elif len(phone_number)!=10:
        return build_validation_result(False, 'phone_number', 'Please type a valid phone number'.format(phone_number))
    
    return build_validation_result(True, None, None)


"""
Performs dialog management and fulfillment for asking details to get restaurant recommendations.
Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
in slot validation and re-prompting.
"""
def get_restaurants(intent_request):    
    source = intent_request['invocationSource']
    
    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        
        time = slots["dining_time"]
        date = slots['dining_date']
        cuisine = slots["cuisine"]
        location = slots["location"]
        people = slots["people"]
        phone_number = slots["phone_number"]
        
        slot_dict = {
            'dining_time': time,
            'dining_date': date,
            'cuisine': cuisine,
            'location': location,
            'people': people,
            'phone_number': phone_number
        }
        
        validation_result = validate_parameters(date, time, cuisine, location, people, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                              intent_request['currentIntent']['name'],
                              slots,
                              validation_result['violatedSlot'],
                              validation_result['message'])


    res = push_to_sqs('https://sqs.us-east-1.amazonaws.com/401380617687/dining_paramq', slot_dict)
    
    if res:
        response = {
            "dialogAction":
                {
                    "fulfillmentState":"Fulfilled",
                    "type":"Close",
                    "message":
                    {
                        "contentType":"PlainText",
                        "content": f"Your request has been received and recommendations will be sent to your number {phone_number}. Have a great day!"
                    }
                }
        }
    else:
        response = {
            "dialogAction":
                {
                    "fulfillmentState":"Fulfilled",
                    "type":"Close",
                    "message":
                    {
                        "contentType":"PlainText",
                        "content": "Sorry, come back after some time!",
                    }
                }
        }
    return response

def lambda_handler(event, context):
    # Set user request on EST by default
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    
    bot_name = event['bot']['name']
    intent_name = event['currentIntent']['name']
    userId = event['userId']

    logger.debug(f'event.bot.name={bot_name}')
    logger.debug(f'dispatch userId={userId}, intentName={intent_name}')
    
    if intent_name == 'diningsuggestion':
        return get_restaurants(event)
    raise Exception('Intent with name ' + intent_name + ' not supported')

