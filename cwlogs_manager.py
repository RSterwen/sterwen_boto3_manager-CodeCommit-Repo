import logging
import random
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# Logger
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()

      
# List Log Groups and Log Streams
def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
    'logGroupNamePrefix': group_name,
    } if group_name else {}
    res = cwlogs.describe_log_groups(**params)
    return res['logGroups']

def list_log_group_streams(group_name, stream_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {'logGroupName': group_name,
        
    } if group_name else {}
    if stream_name:
        params['logStreamNamePrefix'] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res['logStreams']
    

# Filter log events

def filter_log_events(
    group_name, filter_pat,
    start=None, stop=None,
    region_name=None    
):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
        'filterPattern': filter_pat,
    }
    if start:
        params['startTime'] = start
    if stop:
        params['endTime'] = stop
    res = cwlogs.filter_log_events(**params)
    return res['events']
    

# Must convert to timestamp in milliseconds
start_ts = int(datetime(2022, 7, 20, 3, 17, tzinfo=timezone.utc).timestamp() * 1000)
end_ts = int(datetime(2022, 7, 20, 3, 19, tzinfo=timezone.utc).timestamp() * 1000)


if __name__ == '__main__':
    
    list_log_groups=list_log_groups(region_name='ap-southeast-1')
    log.info(f'{list_log_groups}')
    
    list_log_group=list_log_group_streams('/aws/lambda/logtest-Sterwen', region_name='ap-southeast-1')
    log.info(f'{list_log_group}')
    
    filter_events=filter_log_events('/aws/lambda/logtest-Sterwen', 'START',start=start_ts, stop=end_ts,region_name='ap-southeast-1')
    log.info(f'{filter_events}')