import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
import boto3
from botocore.exceptions import ClientError
import operator as op
from boto3.dynamodb.conditions import Key, Attr

# Logger
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()

# CREATE A TOPIC
def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True
    
def list_sns_topics(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)
    
# SUBSCRIBE A TOPIC

def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []), subscriptions.get('NextToken', None)
    
def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
    'TopicArn': topic_arn,
    'Protocol': 'sms',
    'Endpoint': mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    return True
    
    
# PUBLISH A MESSAGE TO A TOPIC

def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {
        'TopicArn': topic_arn,
        'Message': message,
    }
    res = sns.publish(**params)
    print(res)
    return True
    
# UNSUBSCRIBE A TOPIC
def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {
        'SubscriptionArn': subscription_arn,
    }
    res = sns.unsubscribe(**params)
    print(res)
    return True
    
        
# DELETE A TOPIC
def delete_sns_topic(topic_arn):
    # This will delete the topic and all it's subscriptions.
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True
 
 
 
if __name__ == '__main__':
    
    log.info(list_sns_topics())
    
    log.info(create_sns_topic('price_updates_sterwen'))
    
    
    sns_subs_topic = subscribe_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_sterwen','+639176885541')
    log.info(f'{sns_subs_topic}')
    

    sns_sms_message = send_sns_message('arn:aws:sns:ap-southeast-1:337008671328:price_updates_sterwen', 'Woo Hoodies are no 50% off!')
    log.info(f'{sns_sms_message}')
    
    
    log.info(list_sns_subscriptions())
    sns_unsubs_topic = unsubscribe_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_sterwen:199da525-211f-4c97-bfe5-3463687a7b67')
    log.info(f'{sns_unsubs_topic}')
    
    log.info(list_sns_subscriptions())
    sns_delete_topic = delete_sns_topic('arn:aws:sns:ap-southeast-1:337008671328:price_updates_sterwen')
    