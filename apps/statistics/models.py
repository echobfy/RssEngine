import datetime
import mongoengine as mongo
import urllib2
import redis
from django.conf import settings

class MAnalyticsFetcher(mongo.Document):
    date = mongo.DateTimeField(default=datetime.datetime.now)
    feed_id = mongo.IntField()
    feed_fetch = mongo.FloatField()
    feed_process = mongo.FloatField()
    page = mongo.FloatField()
    total = mongo.FloatField()
    server = mongo.StringField()
    feed_code = mongo.IntField()
    
    meta = {
        'db_alias': 'doctopus',
        'collection': 'feed_fetches',
        'allow_inheritance': False,
        'indexes': ['date', 'feed_id', 'server', 'feed_code'],
        'ordering': ['date'],
    }
    
    def __unicode__(self):
        return "%s: %.4s+%.4s+%.4s+%.4s = %.4ss" % (self.feed_id, self.feed_fetch,
                                                    self.feed_process,
                                                    self.page, 
                                                    self.icon,
                                                    self.total)
        
    @classmethod
    def add(cls, feed_id, feed_fetch, feed_process, 
            page, total, feed_code):
        server_name = settings.SERVER_NAME
        if 'app' in server_name: return
        
        if page and feed_process:
            page -= feed_process
        elif page and feed_fetch:
            page -= feed_fetch
        if feed_process and feed_fetch:
            feed_process -= feed_fetch
        
        cls.objects.create(feed_id=feed_id, feed_fetch=feed_fetch,
                           feed_process=feed_process, 
                           page=page, total=total,
                           server=server_name, feed_code=feed_code)
    
    @classmethod
    def calculate_stats(cls, stats):
        return cls.aggregate(**stats)
