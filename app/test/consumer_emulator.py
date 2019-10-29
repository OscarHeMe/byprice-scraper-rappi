#-*- coding: utf-8 -*-
from ByHelpers.rabbit_engine import RabbitEngine
import datetime
from config import *
import json
from pprint import pprint
import pandas as pd
import csv


#Rabbit instances
consumer_routing = RabbitEngine(config={
    					'queue':STREAMER_ROUTING_KEY,
    					'routing_key':STREAMER_ROUTING_KEY
						},blocking=False)
#Product counter
prod_counter = 0

def callback(ch, method, properties, body):
    global cols
    elem = json.loads(body.decode('utf-8'))
    pprint(elem)
    if SCRAPER_TYPE == 'item':
        df = pd.DataFrame([elem])[cols]
        df.to_csv('items_{}.csv'.format(RETAILER_KEY), mode='a', header=False, index=False, quoting=csv.QUOTE_ALL)
    if SCRAPER_TYPE == 'store':
        name = elem.get('name', '').replace(',', '.')
        _id = elem.get('external_id', '')
        addr = elem.get('address', None).replace(',', '')
        lat = str(elem.get('coords', {}).get('lat', '')).replace(',', '')
        lng = str(elem.get('coords', {}).get('lng', '')).replace(',', '')
        els = [name, _id, addr, lat, lng]
        print(els)
        with open('stores_{}.csv'.format(RETAILER_KEY), 'a') as f:
            f.write(','.join(els) + '\n')
    global prod_counter
    prod_counter += 1
    print('Received %d items'%prod_counter)
    # Acknowledgement
    ch.basic_ack(delivery_tag = method.delivery_tag)


if __name__ == '__main__':
    global cols
    cols = []
    consumer_routing.set_callback(callback)
    print("[byprice-data-routing-emulator] INFO: started listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    print('Listening from {}'.format(STREAMER_ROUTING_KEY))
    if SCRAPER_TYPE == 'item':
        cols = ['name', 'description', 'id', 'url']
        with open('items_{}.csv'.format(RETAILER_KEY), 'w') as f:
            f.write('"' + '","'.join(cols) + '"\n')
    if SCRAPER_TYPE == 'store':
        cols = ['name', 'external_id', 'address', 'lat', 'lng']
        with open('stores_{}.csv'.format(RETAILER_KEY), 'w') as f:
            f.write(','.join(cols) + '\n')

    consumer_routing.run()