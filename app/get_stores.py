# -*- coding: utf-8 -*-
import datetime
from pprint import pprint
from worker import app
import pandas as pd

from ByHelpers import applogger
from ByHelpers.rabbit_engine import (MonitorException, stream_info,
                                     stream_monitor)
from ByRequests.ByRequests import ByRequest

from config import OXYLABS, SRV_GEOLOCATION, CELERY_QUEUE

logger = applogger.get_logger()

br = ByRequest(attempts=2)
br.add_proxy(OXYLABS, attempts=5, name="Oxylabs")  


def get_zip():
    z_list = ['01000','44100','64000']#,'76000','50000']
    z_list = list(pd.read_csv('files/zips.csv')['zip'])
    return z_list


def get_stores(params):
    errors = []
    url_zip = "http://" + SRV_GEOLOCATION + "/place/get_places?zip={}"
    br_stats = {}
    try:
        # Obtain Rappi stores for each ZIP
        for zip_code in get_zip():
            logger.debug('[ByRequests] Requesting {}'.format(url_zip.format(zip_code)))
            response = br.get(url_zip.format(zip_code), return_json=True)
            br_stats = br.stats

            if isinstance(response, dict):
                logger.debug('Resp is dict')
                places = response.get('places', [])

                logger.debug(places)
                
                for place in places:
                    gral_data = {
                                "state" : place.get('state'),
                                "country" : "México",
                                "city" : place.get('city'),
                                "zip" : place.get('zip'),
                                "name": 'Rappi ' + raw_st.get('name')
                            }

                    get_stores_from_coords.apply_async(args=(place['lat'], place['lng'], gral_data), queue=CELERY_QUEUE)
                    # get_stores_from_coords(place['lat'], place['lng'], gral_data)
            else:
                err_st = 'Could not get right response from {}'.format(url_zip.format(zip_code))
                errors.append(MonitorException(code=2, reason=err_st))
                logger.error(err_st)

        if len(errors) > 0:
            ws_id = stream_monitor('worker', step='store', ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
            for error in errors:
                stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=error.code, reason=str(error.reason))
        else:
            stream_monitor('worker', step='store', ms_id=params['ms_id'], store_id=params['store_id'])

    except Exception as e:
        ws_id = stream_monitor('worker', step='store', value=1, ms_id=params['ms_id'], store_id=params['store_id'], br_stats=br_stats)
        es_id = stream_monitor('error', ws_id=ws_id, store_id=params['store_id'], code=2, reason=str(e))
        logger.error("Error in : " + str(e))
    return True


def create_st_dict(loc):
    st_id = loc.get('store_id')
    if st_id is not None:
        st_dict = {
            "route_key" : "store",
            "retailer" : "rappi",
            "external_id" : st_id,
            "address" : "",
            "street" : "",
            "phone" : "",
            "date" : str(datetime.datetime.utcnow()),
            "checked" : 0,
            "active" : 1,
            "online" : 1,
            "coords" : {
                "lat" : float(loc['lat']),
                "lng" : float(loc['lng'])
            }
        }
        return st_dict
    else:
        logger.error('Missing store_id')
        return None


def get_stores_from_coords(lat, lng, gral_data={}):
    url_coord = "https://services.mxgrability.rappi.com/api/base-crack/principal?lat={}&lng={}&device=2"
    br = ByRequest(attempts=2)
    br.add_proxy(OXYLABS, attempts=5, name="Oxylabs")
    stores_ls = []
    lat = frmt_coord(lat)
    lng = frmt_coord(lng)
    logger.debug('[ByRequest] Requesting {}'.format(url_coord.format(lat, lng)))
    resp = br.get(url_coord.format(lat, lng), return_json=True)
    if isinstance(resp, list):
        logger.debug('Got response')
        # pprint(resp)
        stores_ls = extract_stores(resp)
    else:
        logger.error('Not a valid response, check if the site changed')
    for raw_st in stores_ls:
        try:
            for loc in raw_st.get('locations', []):
                clean_store = create_st_dict(loc)
                if isinstance(clean_store, dict):
                    clean_store.update(gral_data)
                    stream_info(clean_store)
        except Exception as ex:
            err_st = 'Error with store {}'.format(raw_st)
            errors.append(MonitorException(code=3, reason=err_st))
            logger.error(err_st)

    logger.info('Found {} stores for {}'.format(len(stores_ls), zip_code))
    return stores_ls


def frmt_coord(crd):
    if not isinstance(crd, float):
        try:
            crd = float(crd)
            crd = round(crd, 3)
        except Exception as e:
            logger.error('Could not convert lat to float')
    return crd


def extract_stores(st_raw):
    stores_list = []
    for st_el in st_raw:
        suboptions = st_el.get('suboptions', [])
        if len(suboptions) > 0:
            for store in suboptions:
                loc_list = []
                locations = store.get('stores', [])
                if len(locations) > 0:
                    lat = None
                    lng = None
                    st_id = None
                    for loc in locations:
                        l_dir = {
                            'lat'      : loc.get('lat'),
                            'lng'      : loc.get('lng'),
                            'store_id' : loc.get('store_id')
                        }
                        if l_dir['store_id'] is not None:
                            loc_list.append(l_dir)
                st_dict = {
                    'name' : store.get('name'),
                    'type' : store.get('sub_group'),
                    'locations' : loc_list
                }
                if len(st_dict['locations']) > 0 and str(st_dict['type']).lower() in ['super', 'licores', 'farmacia']:
                    stores_list.append(st_dict)
        else:
            logger.debug('Got no suboptions: {}'.format(st_el.get('name')))
    return stores_list