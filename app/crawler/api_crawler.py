from config import *
import time
import datetime
import random
from pprint import pformat
import requests
from ByRequests.ByRequests import ByRequest
from ByHelpers.rabbit_engine import stream_info
from ByHelpers import applogger

logger = applogger.get_logger()

class StoreCrawler():
    def __init__(self):
        self.br = ByRequest(attempts=1)
        self.br.add_proxy(OXYLABS, attempts=3, name='Oxylabs')
        self._access_token = None
        self.url_auth = 'https://services.mxgrability.rappi.com/api/auth/guest_access_token'
        self.url_content = 'https://services.mxgrability.rappi.com/api/dynamic/context/content'
        self.base_url = 'https://www.rappi.com.mx/'
        self.url_image = 'https://images.rappi.com.mx/products/{}'
        self.url_product = 'https://www.rappi.com.mx/product/{}'
        self.dep_list = []
        self.cat_list = []
        self.product_list = []
        self.total_products = 0

    def perform_request(self, url, headers={}, json={}, method='GET', return_json=True, require_auth=False):
        logger.debug('[ByRequest] Requesting {}'.format(url))
        proxies = {
            "http": OXYLABS,
            "https": OXYLABS,
        }
        global_headers = self.br.headers
        global_headers.update(headers)
        if require_auth:
            global_headers.update({'Authorization': f'Bearer {self.access_token}'})
        response = requests.request(method, url, headers=global_headers, json=json, proxies=proxies)
        # response = self.br.request(method, url, headers=global_headers, json=json)        
        if response.status_code >= 300:
            if require_auth:
                self.get_auth()
                global_headers.update({'Authorization': f'Bearer {self.access_token}'})
            response = requests.request(method, url, headers=global_headers, json=json, proxies=proxies)
            # response = self.br.request(method, url, headers=global_headers, json=json)
        if response and return_json:
            try:
                response = response.json()
            except Exception as e:
                response = {}
        return response
    
    def get_auth(self):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json'
        }
        body = {
            "headers": {
                "normalizedNames": {},
                "lazyUpdate": None
            },
            "grant_type": "guest"
        }
        response = self.perform_request(self.url_auth, method='POST', headers=headers, json=body)
        if response:
            access_token = response.get('access_token')
            self._access_token = access_token
        return access_token

    @property
    def access_token(self):
        if not self._access_token:
            self.get_auth()
        return self._access_token

    def get_store_departments(self, params):
        """
            Method to extract the departments for given store
        """
        headers = {
            'language': 'es',
            'Content-Type': 'application/json',
            'app-version': 'web_4.0.6'
        }
        local_products = []
        try:
            store_id = params['external_id']
            # Prepare request
            url = self.url_content
            body = {
                "state": {
                    "lat": str(params['coords']['lat']),
                    "lng": str(params['coords']['lng']),
                    "parent_store_type": None,
                    "store_type": None
                },
                "limit": 10,
                "offset": 0,
                "context": "store_home",
                "stores": [int(store_id)]
            }
            response = self.perform_request(url, method='POST', headers=headers, json=body, require_auth=True)

            if response:
                # Add departments
                for element in response['data']['components']:
                    if element.get('name') == 'aisles_icons_carousel':
                        for cat in element['resource'].get('aisle_icons', []):
                            self.dep_list.append(self.extract_info(cat))
                        resource_products = element['resource'].get('products', [])
                        local_products.extend(resource_products)

            else:
                err_st = 'Could not get department response for {}'.format(url)
                logger.error(err_st)
            self.product_list.extend(local_products) 
            logger.info('Found {} departments in {} [{}]'.format(len(self.dep_list), params['retailer_key'], store_id))       
        except Exception as e:
            err_st = "Unexpected error in get_store_departments: {}".format(e)
            logger.error(err_st)
            logger.debug(params)
        return self.dep_list

    def get_store_categories(self, params):
        """
            Method to extract the categories for given store
        """
        headers = {
            'language': 'es',
            'Content-Type': 'application/json',
            'app-version': 'web_4.0.6'
        }
        local_products = []
        try:
            store_id = params['external_id']
            # Prepare request
            url = self.url_content
            body = {
                "state": {},
                "limit": 100,
                "offset": 0,
                "context": "aisles_tree",
                "stores": [int(store_id)]
            }
            # body = {
            #     "state": {
            #         "aisle_id": "0",
            #         "parent_id": "0"
            #     },
            #     "limit": 10,
            #     "context": "sub_aisles",
            #     "stores": [int(store_id)]
            # }

            #for i in range(0, len(self.dep_list), 10):
            if True:
                #body["offset"] = i
                response = self.perform_request(url, method='POST', headers=headers, json=body, require_auth=True)

                if response:
                    # Add categories
                    for cat in response['data']['components']:
                        if cat.get('name') == 'aisles_tree':
                            self.cat_list.append(self.extract_info(cat['resource']))
                            resource_products = cat['resource'].get('products', [])
                            local_products.extend(resource_products)

                else:
                    err_st = 'Could not get categories response for store {} - {}'.format(params['retailer_key'], store_id)
                    logger.error(err_st)
                    logger.debug(pformat(body))

            self.product_list.extend(local_products)    
            logger.info('Found {} categories in {} [{}]'.format(len(self.cat_list), params['retailer_key'], store_id))       
        except Exception as e:
            err_st = "Unexpected error in get_store_categories: {}".format(e)
            logger.error(err_st)
            logger.debug(params)
        return self.cat_list


    def get_category_products(self, category_dict, params):
        """
            Method to extract the products for given category
        """
        headers = {
            'language': 'es',
            'Content-Type': 'application/json',
            'app-version': 'web_4.0.6'
        }
        more_items = True
        offset = 0
        category_products = []
        try:
            store_id = params['external_id']
            # Prepare request
            url = self.url_content
            body = {
                "state": {
                    "aisle_id": str(category_dict['id'])
                },
                "limit": 10,
                "context": "aisle_detail",
                "stores": [int(store_id)]
            }

            while more_items:
                local_products = []
                body["offset"] = offset
                response = self.perform_request(url, method='POST', headers=headers, json=body, require_auth=True)

                if response:
                    # Add products
                    for element in response['data']['components']:
                        if element.get('name') == 'aisle_detail':
                            resource_products = element['resource'].get('products')
                            local_products.extend(resource_products)

                else:
                    err_st = 'Could not get product response for category {} - {}'.format(category_dict['name'], category_dict['id'])
                    logger.error(err_st)
                    logger.debug(pformat(body))

                    # retry
                offset += 10

                if len(local_products) < 60:
                    more_items = False

                category_products.extend(local_products)
                
            logger.info('Found {} products in {} | {} [{}]'.format(len(category_products), category_dict['name'], params['retailer_key'], store_id)) 
            self.product_list.extend(category_products)

        except Exception as e:
            err_st = "Unexpected error in get_category_products: {}".format(e)
            logger.error(err_st)
            logger.debug(params)
        return category_products


    def extract_info(self, raw_dep_dict):
        children = raw_dep_dict.get('categories', [])
        if children:
            children = [self.extract_info(child) for child in children]
        n_dict = {
            'name' : raw_dep_dict.get('name'),
            'id': raw_dep_dict.get('id'),
            'children': children,
            'children_count': raw_dep_dict.get('children_count', len(children)),
            'total_products': raw_dep_dict.get('quantity_products')
        }
        return n_dict

    def process_product(self, params, raw_prod):
        clean_product = {
            'route_key' : params['route_key'],
            'retailer' : params['retailer_key'],
            'name' : raw_prod.get('name'),
            'id' :  raw_prod.get('product_id'),
            'url' : self.url_product.format(raw_prod.get('id')),
            'gtin' : raw_prod.get('ean'),
            'date' : str(datetime.datetime.utcnow()),
            'description' : raw_prod.get('description'),
            'brand' : raw_prod.get('trademark'),
            'provider' : '',
            'ingredients' : [],
            'images' : [
                self.url_image.format(raw_prod.get('image'))
            ],
            'raw_attributes' : [
                {
                    'key'  : 'content',
                    'value': raw_prod.get('quantity'),
                    'unit' : raw_prod.get('unit_type')
                }
            ],
            'raw_ingredients' : '',
            'price' : float(raw_prod.get('price')) if raw_prod.get('price') is not None else None,
            'price_original' : float(raw_prod.get('real_price')) if raw_prod.get('real_price') is not None else None,
            'discount' : float(raw_prod.get('discount')) if raw_prod.get('discount') is not None else None,
            'promo' : '',
            'location' : {
                'store' : [
                    params['store_uuid']
                ]
            }
        }
        return clean_product
            
    def send_products(self, params):
        logger.info('Found {} products to send {} [{}]'.format(len(self.product_list), params['retailer_key'], params['external_id']))
        for product in self.product_list:
            clean_product = self.process_product(params, product)
            try:
                stream_info(clean_product)
                self.total_products += 1
            except Exception as e:
                err_str = 'Could not send product_id {}: {}'.format(clean_product.get('id'), e)
                logger.error(err_str)
        logger.info('Sent {} products {} [{}]'.format(self.total_products, params['retailer_key'], params['external_id']))
        

    def crawl_store(self, params):
        # self.get_store_departments(params)
        # time.sleep(1)
        self.get_store_categories(params)
        # time.sleep(0.5)
        # for category_dict in self.cat_list:
        #     for subcategory_dict in category_dict['children']:
        #         self.get_category_products(subcategory_dict, params)
        #     time.sleep(random.randint(2, 4))
        # self.send_products(params)
        # return self.total_products
        return 0
