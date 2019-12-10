# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
This Cloud function is responsible for:
- Export database data from Cloud SQL MySQL into a Cloud Storage Bucket
'''


import base64
import logging
import json

from datetime import datetime
from httplib2 import Http

from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials


def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
         
    Event Payload Sample:
    	{
          "db":"[DATABASE NAME]",
          "instance":"[SQL INSTANCE ID]",
          "project":"[PROJECT_ID]",
          "gs":"gs://[BUCKET_NAME]"
        }
    """
    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    credentials = GoogleCredentials.get_application_default()
    
    service = discovery.build(
        'sqladmin',
        'v1beta4',
        http=credentials.authorize(Http()),
        cache_discovery=False
    )
    
    datestamp = datetime.now().strftime("%Y%m%d%H%M") # format timestamp: YearMonthDayHourMinute
    uri = "{0}/backup-{1}-{2}.gz".format(pubsub_message['gs'], pubsub_message['db'], datestamp)
    
    instances_export_request_body = {
      "exportContext": {
        "kind": "sql#exportContext",
        "fileType": "SQL",
        "uri": uri,
        "databases": [
          pubsub_message['db']
        ]
      }
    }
    
    try:
    	request = service.instances().export(
    		project=pubsub_message['project'],
        	instance=pubsub_message['instance'],
        	body=instances_export_request_body
    	)
    	response = request.execute()
    except HttpError as err:
        logging.error("Could NOT run backup. Reason: {}".format(err))
    else:
    	logging.info("Backup task status: {}".format(response))    

