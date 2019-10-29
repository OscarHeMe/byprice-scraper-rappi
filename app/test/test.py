# -*- coding: utf-8 -*-
import unittest
import worker
from app import get_stores
from pprint import pprint
from ByHelpers.rabbit_engine import stream_info


master_id, st_id = 'fake_ms', 'fake_st'
    
params = {
    'route_key'   : 'price',
    'retailer_key': 'rappi',
    'external_id' : '990008569',
    'coords'      : {
        'lat': '19.432559',
        'lng': '-99.133247'
    },
    'store_uuid'  : 'fake_st_uuid',
    'ms_id'       : master_id,
    'store_id'    : st_id
}

dep_2_crwl = {
    'id': 292,
    'name': 'Suplementos',
    'sub_dep': [
        {'id': 15725, 'name': 'Suplementos'},
        {'id': 15726, 'name': 'Suplementos Nutricionales'},
        {'id': 15727, 'name': 'Vitaminas y Minerales'}
    ]
} 


class RappiTestCase(unittest.TestCase):
    """Rappi unit tests"""

    @unittest.skip('Already tested')
    def test_1_get_stores(self):
        print("\n******************Located Stores*******************\n")
        output = get_stores.get_stores_from_coords(params['coords']['lat'], params['coords']['lng'])
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

    @unittest.skip('Already tested')
    def test_2_start_stores(self):
        print("\n******************Get Stores*******************\n")
        out = worker.start_stores(master_id, params)
        self.assertTrue(out)
    
    @unittest.skip('Already tested')
    def test_3_get_deps(self):
        print("\n******************Get Departments*******************\n")
        output = worker.get_store_deps(params)
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

    @unittest.skip('Already tested')
    def test_4_crawl_cats(self):
        print("\n******************Crawl Categories*******************\n")
        output = worker.crawl_cat(dep_2_crwl['name'], dep_2_crwl['sub_dep'][0], params, run_all=False)
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

    # @unittest.skip('Already tested')
    def test_1_get_stores(self):
        print("\n******************Located Stores*******************\n")
        output = get_stores.get_stores(params)
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))


    def test_6_send_items(self):
        print("\n******************Send items*******************\n")
        count = 1
        while True:
            prod_cl = {
                "name": "Sodimel 30 Capsulas  ",
                "route_key": "item",
                "id": "975728256",
                "price_original": 628.6,
                "provider": "",
                "location": {
                    "store": ["4eba1f56-dbd3-11e9-8f82-0242ac110002"]
                }, "raw_attributes": [{"unit": "U", "key": "content", "value": 1}],
                "retailer": "rappi",
                "promo": "",
                "url": "https://www.rappi.com.mx/product/888002_975728256",
                "description": "Sodimel 30 Capsulas {}".format(count),
                "discount": 0.0,
                "categories": ["Farmacia", "Medicamentos"],
                "price": 707.18,
                "brand": "Costco",
                "date": "2019-09-24 20:48:52.701897",
                "ingredients": [],
                "gtin": "74849900131",
                "raw_ingredients": "",
                "images": ["https://images.rappi.com.mx/products/975728256-1544465989.jpg"]
            }
            if count % 100.0 == 0:
                prod_cl.pop('images')

            stream_info(prod_cl)
            print('COUNT', count)
            #if count == 300:
            #    break

            count += 1

        

if __name__ == "__main__":
    unittest.main()
