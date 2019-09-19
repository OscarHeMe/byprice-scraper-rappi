# -*- coding: utf-8 -*-
import datetime
from ByHelpers import applogger
from ByHelpers.rabbit_engine import stream_info, MonitorException, stream_monitor
from ByRequests.ByRequests import ByRequest
from celery import Celery
from app.get_stores import get_stores
from config import *

# logging
applogger.create_logger()
logger = applogger.get_logger()

# Variables
RETAILER = 'rappi'
base_url = 'https://www.rappi.com.mx/'
url_store = 'https://services.mxgrability.rappi.com/windu/corridors/sub_corridors/store/{}'
url_cat = 'https://services.mxgrability.rappi.com/api/subcorridor_sections/products?subcorridor_id={}&store_id={}&include_stock_out=true&limit={}'
url_image = 'https://images.rappi.com.mx/products/{}'
url_product = base_url + 'product/{}'

LIMIT = 200

broker = 'amqp://{}:{}@{}:{}/{}'.format(
                CELERY_USER, CELERY_PASS, CELERY_BROKER, CELERY_PORT, CELERY_VIRTUAL_HOST)

queue = CELERY_QUEUE

# Celery app initilaization
app = Celery('worker', broker=broker)
app.conf.task_routes = {'worker.*': {'queue': queue}}
app.conf.enable_utc = True
app.conf.CELERY_ACCEPT_CONTENT = ['json']
app.conf.CELERY_RESULT_SERIALIZER = 'json'
app.conf.CELERY_TASK_SERIALIZER = 'json'

app.conf.update({
    'worker_hijack_root_logger': False, # so celery does not set up its loggers
    'worker_redirect_stdouts': False, # so celery does not redirect its logs
})


@app.task
def start_stores(master_id, st_id):
    monitor_dict = { 
        'ms_id'   : master_id,
        'store_id': st_id,
    }
    return get_stores(monitor_dict)


@app.task
def crawl_store(master_id, params):
    monitor_dict = { 
        'ms_id'   : master_id,
        'store_id': params['store_uuid'],
    }
    params.update(monitor_dict)
    logger.debug(params)
    all_deps = get_store_deps(params)
    for dep in all_deps:
        scount = 0
        for scat in dep['sub_dep']:
            scount += 1
            # crawl_cat(dep['name'], scat, params)
            crawl_cat.apply_async(args=(dep['name'], scat, params), queue=CELERY_QUEUE)
        logger.info('{} Subcategories in {}'.format(scount, dep['name']))
    logger.info('Found {} departments in store {}'.format(len(all_deps), params['name']))
    return all_deps



def get_store_deps(params):
    """
        Method to extract the departments in retailer
    """
    dep_list = []
    br_stats = {}
    try:
        store_id = params['external_id']
        # Prepare request
        br = ByRequest(attempts=1)
        br.add_proxy(OXYLABS, attempts=3, name='Oxylabs')
        logger.debug('[ByRequest] Rquesting {}'.format(url_store.format(store_id)))
        response = br.get(url_store.format(store_id), return_json=True)
        br_stats = br.stats
        ws_id = stream_monitor('worker', step='start', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)

        if response:
            # Add departments
            for dep in response:
                if (int(dep.get('index', 0)) > 1) and (int(dep.get('index', 0)) < 100):
                    dep_list.append(extract_info(dep))
        else:
            err_st = 'Could not get response for {}'.format(url_store.format(store_id))
            logger.error(err_st)
            stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=0, reason=str(err_st))
                
    except Exception as e:
        err_st = "Unexpected error in get_store_deps: {}".format(e)
        ws_id = stream_monitor('worker', step='start', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=err_st)
        logger.error(err_st)
        logger.debug(params)
    return dep_list



def extract_info(raw_dep_dict):
    sub_cat_ls = raw_dep_dict.get('sub_corridors', [])
    sdep = [extract_info(scat) for scat in sub_cat_ls]
    n_dict = {
        'name'   : raw_dep_dict.get('name'),
        'id'     : raw_dep_dict.get('id')
    }
    if len(sdep) > 0:
        n_dict.update({
            'sub_dep' : sdep
        })
    return n_dict


@app.task
def crawl_cat(dep_name, scat, params, page=1, run_all=True):
    br_stats = {}
    br = ByRequest(attempts=1)
    br.add_proxy(OXYLABS, attempts=3, name='Oxylabs')
    errors = []

    # Url creation
    url = url_cat.format(scat['id'], params['external_id'], LIMIT)
    if page != 1:
        url = url + '&next_id={}'.format(LIMIT)

    logger.debug('[ByRequest] Requesting {}'.format(url))
    
    try:
        response = br.get(url, return_json=True)
        br_stats = br.stats
    
        prod_raw_ls = []
        prods_ls = []
        cat_ls = [dep_name, scat['name']]

        # Product list extraction
        if isinstance(response, dict):
            result = response.get('results', [])
            for res in result:
                prod_raw_ls.extend(res.get('products', []))
        else:
            err_st = 'Could not get response from {}'.format(url)
            logger.error(err_st)
            errors.append(MonitorException(code=0, reason=err_st))

        # Check if there are more products to crawl
        n_prod = len(prod_raw_ls)
        logger.info('Found {} products in page {} for {}'.format(n_prod, page, ' - '.join(cat_ls)))

        if (n_prod == LIMIT) and run_all:
            logger.debug('Found next page...')
            # crawl_cat(dep_name, scat, params, page=page+1)
            crawl_cat.apply_async(args=(dep_name, scat, params, page+1), queue=CELERY_QUEUE)
        for prod in prod_raw_ls:
            try:
                prod_clean = process_prod(prod, params)
                if prod_clean:
                    prod_clean.update({
                        'categories': cat_ls,
                    })
                    prods_ls.append(prod_clean)
                    logger.info('Streaming')
                    stream_info(prod_clean)
                else:
                    err = 'Could not get product'
                    logger.error(err)
                    raise Exception(err)
            except Exception as exe:
                err_st = 'Error with product: {}'.format(prod)
                logger.error(err_st)
                errors.append(MonitorException(code=2, reason=err_st))
                
        if len(errors) > 0:
            ws_id = stream_monitor('worker', step='category', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
            for error in errors:
                stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=error.code, reason=str(error.reason))
        else:
            stream_monitor('worker', step='category', value=1, ms_id=params['ms_id'], store_id=params['store_id'])

    except Exception as e:
        err_st = "Unexpected error in crawl_cat: {}".format(e)
        ws_id = stream_monitor('worker', step='category', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=err_st)
        logger.error(err_st)
    return prods_ls



def process_prod(raw_prod, params):
    prod_cl = {}
    ws_id = stream_monitor('worker', step=params.get('route_key'), value=1, ms_id=params['ms_id'], store_id=params['store_id'])
    try:
        prod_cl = {
            'route_key' : params['route_key'],
            'retailer' : params['retailer_key'],
            'name' : raw_prod.get('name'),
            'id' :  raw_prod.get('product_id'),
            'url' : url_product.format(raw_prod.get('id')),
            'gtin' : raw_prod.get('ean'),
            'date' : str(datetime.datetime.utcnow()),
            'description' : raw_prod.get('description'),
            'brand' : raw_prod.get('trademark'),
            'provider' : '',
            'ingredients' : [],
            'images' : [
                url_image.format(raw_prod.get('image'))
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


        
        # {
        #     'age_restriction': False,
        #     'balance_price': 374,
        #     'cm_height': 15,
        #     'cm_width': 7.07,
        #     'description': 'Royal Canin Pug Adulto',
        #     'discount': 0,
        #     'discount_earnings': '',
        #     'discount_pay_products': '',
        #     'discount_tag_color': '',
        #     'discount_type': '',
        #     'ean': '30111454409',
        #     'has_antismoking': False,
        #     'has_pum': True,
        #     'has_toppings': False,
        #     'have_discount': False,
        #     'height': 520,
        #     'id': '72000175_975352638',
        #     'identification_required': False,
        #     'image': '975352638-1537993139.png',
        #     'in_stock': True,
        #     'is_available': True,
        #     'is_discontinued': False,
        #     'label': None,
        #     'max_quantity_in_grams': 0,
        #     'min_quantity_in_grams': 0,
        #     'name': 'Royal Canin Pug Adulto',
        #     'need_image': False,
        #     'price': 374,
        #     'product_groups_tags': [],
        #     'product_id': '975352638',
        #     'pum': '300.88',
            
        #     'real_balance_price': 340,
        #     'real_price': 340,
        #     'requires_medical_prescription': False,
        #     'sale_type': 'U',
        #     'step_quantity_in_grams': 0,
        #     'store_id': 72000175,
        #     'store_type': 'petco',
        #     'trademark': 'Royal Canin',
        #     'unit_type': 'Kg',
        #     'quantity': 1.13,
        #     'width': 245
        # }
        logger.debug(prod_cl)
    except Exception as e:
        err_st = "Unexpected error in process_prod: {}".format(e)
        ws_id = stream_monitor('worker', step=params.get('route_key'), value=1, ms_id=params['ms_id'], store_id=params['store_id'])
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=err_st)
        logger.error(err_st)
    return prod_cl