#!/usr/bin/env python

import logging
import sys
import os
import requests
import json

logging.basicConfig(stream=sys.stderr, level=logging.INFO)

### Constants
RW_API = 'https://api.resourcewatch.org/v1'
QUERY_URL = RW_API + "/query/{dataset_id}?sql={sql}"
LAYER_URL = RW_API + "/dataset/{dataset_id}/layer/{layer_id}"
LAYER_IDS = [
    'f4897107-5ae5-4685-8eee-cd1a5745a384',
    '275dcc83-673b-44e4-b7db-253ff1d2d867',
    '9bcaa1e8-3181-441f-879a-060b068b7c2a',
    '80d2665b-bba4-4de9-ba5e-d0487e920784',
    '4d52872e-0653-4b28-a1a6-dd4edbb76dd3',
    'b92c01ee-eb2c-4835-8625-d138db75a1cd',
    '31429259-9a9a-4d66-a1b9-92c08aa407f3',
    'a2eccfd8-de7e-4fb8-93c4-22f119994f3e',
    '2da3bbb8-a8b7-47b7-b3bc-823ddc330960',
    '0c094e37-4563-4633-9a38-28dd4a4724bf',
    'd0ec0531-9241-407c-bbae-d3dc55c7d6ea',
    '5ca12eec-f8fe-49eb-b353-67c9eeb5bc6a',
    '61067a0d-b2a3-441e-85c1-2eef5a18e4a5',
    'd63fff22-8cda-467e-b4ef-df3ab2613505',
    'a64f5142-e8ae-433f-afda-6628fc3255bf',
    '509c71b2-9c68-4acd-88a6-f64015494001',#SO2
    '39524ca8-e1e3-4e9f-8b77-6682064bd176',#O3
    '5b5c7d9b-baf3-4fdf-a41c-e10506b72770',#NO2
    '51861c34-f67a-4662-b0b6-1b7f265c6d23', #CO
    '8f848d9d-0d82-4976-a522-cdbf741c25f8' #BC
]
API_KEY = os.environ['RW_API_KEY']
INTERSECTS_SQL = "st_intersects(the_geom,st_buffer(ST_SetSRID(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":{{point}}}'),4326),1))"

def getLayer(layer_id):
    url = "{api_url}/layer/{layer_id}".format(
        api_url=RW_API, layer_id=layer_id)
    response = requests.request('GET', url)
    if not response.ok:
        logging.error("ERROR: failed to update layer")
        logging.error(response.text)
    else:
        return response.json()['data']

def main():
    logging.info('BEGIN')

    for layer_id in LAYER_IDS:
        layer = getLayer(layer_id)

        dataset_id = layer['attributes']['dataset']
        if layer['attributes']['provider'] == 'cartodb':
            config_sql = layer['attributes']['layerConfig']['body']['layers'][0]['options']['sql']
            where_pos = config_sql.lower().find('where')
            if where_pos >= 0:
                sql = '{} {} AND{}'.format(
                    config_sql[:where_pos+5],
                    INTERSECTS_SQL,
                    config_sql[where_pos+5:]
                )
            else:
                sql = '{} WHERE {}'.format(config_sql, INTERSECTS_SQL)

            interaction = layer['attributes']['interactionConfig']
            interaction["pulseConfig"] = {
                "url": QUERY_URL.format(dataset_id=dataset_id, sql=sql)
            }
            payload = {
                'application': ['rw'],
                'interactionConfig': interaction
            }
            post_url = LAYER_URL.format(dataset_id=dataset_id, layer_id=layer_id)
            post(post_url, payload)
            logging.info('Updated {}'.format(layer_id))
        else:
            logging.info('Not carto')


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
        logging.error("ERROR: failed to update layer")
        logging.error(response.text)


if __name__ == "__main__":
    main()
