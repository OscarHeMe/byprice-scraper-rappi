#-*- coding: utf-8 -*-
from ByHelpers.rabbit_engine import RabbitEngine
import datetime
from config import *
import json
from pprint import pprint


#Rabbit instances
consumer_routing = RabbitEngine(config={
    					'queue':STREAMER_ROUTING_KEY,
    					'routing_key':STREAMER_ROUTING_KEY
						},blocking=False)
#Product counter
prod_counter = 0

def callback(ch, method, properties, body):
    elem = json.loads(body.decode('utf-8'))
    pprint(elem)
    if SCRAPER_TYPE == 'price':
        name = elem.get('name', '').replace(',', '.')
        _id = elem.get('id', '')
        url = elem.get('url', '')
        desc = elem.get('description', '')
        attrs = elem.get('raw_attributes', [{}])[0]
        unit, value = attrs.get('unit'), attrs.get('value')
        els = [str(name).replace('\n', ''), str(_id), str(desc).replace('\n', ''), str(unit), str(value)]
        if (unit is not None) and (value is not None):
            with open('items_{}.csv'.format(RETAILER_KEY), 'a') as f:
                f.write('|'.join(els) + '\n')
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
    consumer_routing.set_callback(callback)
    print("[byprice-data-routing-emulator] INFO: started listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    print('Listening from {}'.format(STREAMER_ROUTING_KEY))
    if SCRAPER_TYPE == 'price':
        cols = ['name', 'id', 'description', 'content_unit', 'content_value']
        with open('items_{}.csv'.format(RETAILER_KEY), 'w') as f:
            f.write('|'.join(cols) + '\n')
    if SCRAPER_TYPE == 'store':
        cols = ['name', 'external_id', 'address', 'lat', 'lng']
        with open('stores_{}.csv'.format(RETAILER_KEY), 'w') as f:
            f.write(','.join(cols) + '\n')

    consumer_routing.run()