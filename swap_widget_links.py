#!/usr/bin/env python

import logging
import sys
import os
import requests
import json

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

### Constants
RW_API = 'https://api.resourcewatch.org/v1'
URL = RW_API + "/dataset/{dataset_id}/widget/{widget_id}/metadata"

API_KEY = os.environ['RW_API_KEY']

def getWidgets():
    url = "{api_url}/widget/?app=rw&published=true&includes=metadata&page[size]=1000".format(api_url=RW_API)
    response = requests.request('GET', url)
    if not response.ok:
        logging.error(response.text)
    else:
        return response.json()['data']


def main():
    logging.info('BEGIN')

    widgets = getWidgets()

    for widget in widgets:
        metadata = widget['attributes']['metadata']

        if len(metadata):
            for m in metadata:
                if ('info' in m['attributes'] and
                    'widgetLinks' in m['attributes']['info']):
                    for l in m['attributes']['info']['widgetLinks']:
                        staging = l['link'].find('staging.')
                        if (staging > -1):
                            new_link = l['link'][:staging] + l['link'][staging+8:]
                            l['link'] = new_link
                            widget_id = widget['id']
                            dataset_id = widget['attributes']['dataset']
                            url = URL.format(dataset_id=dataset_id,
                                             widget_id=widget_id)
                            post(url, m['attributes'])


def post(url, payload):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer {token}'.format(token=API_KEY)
    }
    response = requests.request(
        'PATCH',
        url,
        data=json.dumps(payload),
        headers=headers
    )
    if not response.ok:
        logging.error("ERROR: failed to patch")
        logging.error(response.text)


if __name__ == "__main__":
    main()
