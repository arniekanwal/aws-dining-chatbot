import boto3
import dateutil.parser
import datetime
import time
import os
import logging
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """

def push_to_sqs(msg_body, request_id):
    sqs = boto3.client('sqs')
    queue_url = f"https://sqs.us-east-1.amazonaws.com/{user_id}/dining_paramq.fifo"

    response = sqs.send_message(
        QueueUrl=queue_url,
        DelaySeconds=0,
        MessageAttributes={
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
            'dining_date': {
                'DataType': 'String',
                'StringValue': msg_body['dining_time']
            },
            'people': {
                'DataType': 'Number',
                'StringValue': msg_body['people']
            }
        },
        MessageBody=f"Queried for cuisine:{msg_body['cuisine']} in location:{msg_body['location']}",
        MessageGroupId=request_id,
        MessageDeduplicationId=str(uuid4()),
    )

    print(f"SQS Response: {response}")


def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


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


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


""" --- Helper Functions --- """

def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def isvalid_date(date):
    verbal_dates = ['today', 'tomorrow', 'tmrw', 'day after tomorrow', 'day after tmrw']
    if date.lower() in verbal_dates:
        return True
    
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
    
def isvalid_time(time):
    try:
        datetime.datetime.strptime(time, '%H:%M')
        return True
    except ValueError:
        return False
    
    
def isvalid_city(city):
    valid_cities = ['new york', 'nyc', 'ny', 'manhattan']
    return city.lower() in valid_cities


def isvalid_cuisine(cuisine):
    cuisines = ["thai", "chinese", "mexican", "indian", "sushi", "italian", "pizza", "french", "ramen", "korean"]
    return cuisine.lower() in cuisines


def validate_dining(location, cuisine, dining_date, dining_time, people, phone_number):
    if location is not None:
        if not isvalid_city(location.lower()):
            return build_validation_result(False, 'location', 'We currently do not offer recommendations for that city. Would you like to try somewhere else?')

    if cuisine is not None:
        if not isvalid_cuisine(cuisine.lower()):
            return build_validation_result(False, 'cuisine', 'We currently do not offer recommendations for that cuisine. Would you like to try something else?')    

    if dining_date is not None:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'dining_date', 'I did not understand that, what date do you want to reserve')
        elif datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'dining_date', 'You can make a reservation from today onwards. What day would you prefer?')
        
    if phone_number is not None:
        if len(phone_number) != 10:
            return build_validation_result(False, 'phone_number', "Please enter a valid phone number.")
        
    if dining_time is not None and not isvalid_time(dining_time):
        return build_validation_result(False, 'dining_time', "Enter a valid time.")
        
    if people is not None and not parse_int(people):
        return build_validation_result(False, 'count', 'Sorry, I could not understand that. How many guests will be attending?')

    return build_validation_result(True, None, None)


""" --- Functions that control the bot's behavior --- """

def gather_dining_info(intent_request):
    source = intent_request['invocationSource']    
    slots = get_slots(intent_request)
    
    dining_time = slots["dining_time"]
    cuisine = slots["cuisine"]
    location = slots["location"]
    people = slots["people"]
    dining_date = slots['dining_date']
    phone_number = slots["phone_number"]
    
    slot_dict = {
        'dining_time': dining_time,
        'dining_date': dining_date,
        'cuisine': cuisine,
        'location': location,
        'people': people,
        'phone_number': phone_number
    }

    if source == 'DialogCodeHook':
        validation_result = validate_dining(location, cuisine, dining_date, dining_time, people, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        # continue to elicit slots if needed
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return delegate(output_session_attributes, get_slots(intent_request))

    # Fulfillment code hook, send information to queue
    request_id='myuser'
    # print(intent_request)
    # print(slot_dict)
    push_to_sqs(slot_dict, request_id)
    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'Thanks, your recommendations will be texted to you shortly!!'})


""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningIntent':
        return gather_dining_info(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)





