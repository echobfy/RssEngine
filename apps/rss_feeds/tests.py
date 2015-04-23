from django.test import TestCase

from models import Feed
from utils import feed_fetcher
import redis

class FeedFetcherTest(TestCase):

    def setUp(self):
    	Feed.objects.create(feed_address='http://rss.sina.com.cn/news/china/focus15.xml', last_update='2015-04-23 12:11:18')

    def tearDown(self):
    	r = redis.Redis('localhost', db=4)
    	r.flushdb();
    
    def testDispatcher(self):
    	options = {
            'verbose': False,
            'timeout': 10,
            'single_threaded': True
        }
        self.disp = feed_fetcher.Dispatcher(options, 1)
        self.disp.add_jobs([[1]])
        feed = self.disp.run_jobs()
        self.assertEqual(feed.pk, 1, 'pk is not equal')
