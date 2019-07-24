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
    global prod_counter
    prod_counter += 1
    print('Received %d items'%prod_counter)
    # Acknowledgement
    ch.basic_ack(delivery_tag = method.delivery_tag)


if __name__ == '__main__':
    consumer_routing.set_callback(callback)
    print("[byprice-data-routing-emulator] INFO: started listener (" + STREAMER_ROUTING_KEY + ") at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    consumer_routing.run()
