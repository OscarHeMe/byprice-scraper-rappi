# -*- coding: utf-8 -*-
import unittest
import worker
from app import get_stores
from app.crawler.api_crawler import StoreCrawler
from pprint import pprint
from ByHelpers.rabbit_engine import stream_info
from ByHelpers import applogger

logger = applogger.get_logger()


master_id, st_id = 'fake_ms', 'fake_st'
    
params = {
    'route_key'   : 'price',
    'retailer_key': 'rappi',
    'external_id' : '139123485',
    'coords'      : {
        'lat': 19.5023,
        'lng': -99.2523,
    },
    'store_uuid'  : 'fake_st_uuid',
    'ms_id'       : master_id,
    'store_id'    : st_id
}


dep_2_crwl = {
    'id': 23859,
    'name': 'Medicamentos',
    'sub_dep': [
        {'id': 43759, 'name': 'Medicamentos Eticos'}
    ]
}

category_dict =  {
    'children_count': None,
    'id': 29810,
    'name': 'Huesos y Articulaciones',
    'total_products': None
}


class RappiTestCase(unittest.TestCase):
    """Rappi unit tests"""

    def setUp(self):
        worker.app.conf.update(CELERY_ALWAYS_EAGER=True)

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
        crawler = StoreCrawler()
        output = crawler.get_store_departments(params)
        pprint(output)
        output = crawler.get_store_categories(params)
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

    # @unittest.skip('Already tested')
    # def test_4_crawl_cats(self):
    #     print("\n******************Crawl Categories*******************\n")
    #     output = worker.crawl_cat(dep_2_crwl['name'], dep_2_crwl['sub_dep'][0], params, run_all=True)
    #     pprint(output)
    #     self.assertTrue(isinstance(output, list) and (len(output) > 0))

    @unittest.skip('Already tested')
    def test_1_get_stores(self):
        print("\n******************Located Stores*******************\n")
        output = get_stores.get_stores(params)
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

    @unittest.skip('Already tested')
    def test_6_send_items(self):
        print("\n******************Crawl products*******************\n")
        crawler = StoreCrawler()
        output = crawler.get_category_products(category_dict, params)
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

    # @unittest.skip('Already tested')
    def test_7_crawl_store(self):
        print("\n******************Crawl Store*******************\n")
        crawler = StoreCrawler()
        output = crawler.crawl_store(params)
        pprint(output)
        self.assertTrue(isinstance(output, int) and (output > 0))

    @unittest.skip('Already tested')
    def test_8_pr_zip(self):
        print("\n******************Process Zip*******************\n")
        output = worker.process_zip('06600')
        pprint(output)
        self.assertTrue(isinstance(output, list) and (len(output) > 0))

        

if __name__ == "__main__":
    unittest.main()
