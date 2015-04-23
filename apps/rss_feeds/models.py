import difflib
import datetime
import time
import random
import re
import math
import mongoengine as mongo
import zlib
import hashlib
import redis
import pymongo
import sys
from collections import defaultdict
from operator import itemgetter
from bson.objectid import ObjectId
from BeautifulSoup import BeautifulSoup
from django.db import models
from django.db import IntegrityError
from django.conf import settings
from django.db.models.query import QuerySet
from django.core.urlresolvers import reverse
from django.contrib.sites.models import Site
from django.template.defaultfilters import slugify
from mongoengine.queryset import OperationError, Q, NotUniqueError
from mongoengine.base import ValidationError
from vendor.timezones.utilities import localtime_for_timezone
from apps.rss_feeds.tasks import UpdateFeeds
from apps.rss_feeds.text_importer import TextImporter
from utils import feedfinder, feedparser
from utils import urlnorm
from utils import log as logging
from utils.feed_functions import levenshtein_distance
from utils.feed_functions import timelimit, TimeoutError
from utils.feed_functions import relative_timesince
from utils.feed_functions import seconds_timesince
from utils.story_functions import strip_tags, htmldiff, strip_comments, strip_comments__lxml
import pytz
from apps.search.models import SearchStory
#from fdfs_client.client import Fdfs_client,DataError
#from PIL import Image
import cStringIO
import urllib2
import traceback
from django.core.mail import mail_admins
from utils.image_url_filters import IMAGE_BLACKLIST_FILTERS


ENTRY_NEW, ENTRY_UPDATED, ENTRY_SAME, ENTRY_ERR = range(4)

class Feed(models.Model):
    feed_address = models.URLField(max_length=764, db_index=True)
    feed_link = models.URLField(max_length=1000, default="", blank=True, null=True)
    hash_address_and_link = models.CharField(max_length=64, unique=True)  # hashlib.sha1(feed_address + feed_link)
    feed_title = models.CharField(max_length=255, default="[Untitled]", blank=True, null=True)

    last_update = models.DateTimeField(db_index=True)               # the last update time for the record of feed
    next_scheduled_update = models.DateTimeField()                  # the next time to schedule this feed_id
    fetched_once = models.BooleanField(default=False)               # fetched once before
    known_good = models.BooleanField(default=False)                 # the feed is not good feed, after the fetch is OK, then good

    has_feed_exception = models.BooleanField(default=False, db_index=True)
    exception_code = models.IntegerField(default=0)
    errors_since_good = models.IntegerField(default=0)              # it means number of errors since the last a good fetch

    min_to_decay = models.IntegerField(default=0)                   # according to stories_last_month, error_count,
                                                                    # calculate the baseline time to next scheduled time
    creation = models.DateField(auto_now_add=True)                  # when the feed created

    etag = models.CharField(max_length=255, blank=True, null=True)  # etag stands for entity tag, see HTTP for more.
    last_modified = models.DateTimeField(null=True, blank=True)     # function similar to etag

    stories_last_month = models.IntegerField(default=0)             # the number of stories about this feed_id las month.
    last_story_date = models.DateTimeField(null=True, blank=True)   # the last story date
    last_load_time = models.IntegerField(default=0)                 # the last cost time of fetching feed.link

    class Meta:
        db_table = "feeds"
        ordering = ["feed_title"]

    def __unicode__(self):
        if not self.feed_title:
            self.feed_title = "[Untitled]"
            self.save()
        return "%s (%s)" % (
            self.feed_title,
            self.pk)

    @property
    def title(self):
        return self.feed_title or "[Untitled]"

    @property
    def permalink(self):
        return "site/%s/%s" % (self.pk, slugify(self.feed_title.lower()[:50]))

    @property
    def error_count(self):
        r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)
        fetch_errors = int(r.zscore('error_feeds', self.pk) or 0)

        return fetch_errors + self.errors_since_good


    def canonical(self, full=False):
        feed = {
            'id': self.pk,
            'feed_title': self.feed_title,
            'feed_address': self.feed_address,
            'feed_link': self.feed_link,
            'updated': relative_timesince(self.last_update),
            'updated_seconds_ago': seconds_timesince(self.last_update),
            'min_to_decay': self.min_to_decay,
            'fetched_once': self.fetched_once,
            'not_yet_fetched': not self.fetched_once,  # Legacy. Doh.
        }

        if self.has_feed_exception:
            feed['has_exception'] = True
            feed['exception_type'] = 'feed' if self.has_feed_exception else 'page'
            feed['exception_code'] = self.exception_code
        elif full:
            feed['has_exception'] = False
            feed['exception_type'] = None
            feed['exception_code'] = self.exception_code

        return feed

    def save(self, *args, **kwargs):
        if not self.last_update:
            self.last_update = datetime.datetime.utcnow()
        if not self.next_scheduled_update:
            self.next_scheduled_update = datetime.datetime.utcnow()
        self.fix_google_alerts_urls()

        feed_address = self.feed_address or ""
        feed_link = self.feed_link or ""
        self.hash_address_and_link = hashlib.sha1(feed_address + feed_link).hexdigest()

        max_feed_title = Feed._meta.get_field('feed_title').max_length
        if len(self.feed_title) > max_feed_title:
            self.feed_title = self.feed_title[:max_feed_title]
        max_feed_address = Feed._meta.get_field('feed_address').max_length
        if len(feed_address) > max_feed_address:
            self.feed_address = feed_address[:max_feed_address]
        max_feed_link = Feed._meta.get_field('feed_link').max_length
        if len(feed_link) > max_feed_link:
            self.feed_link = feed_link[:max_feed_link]

        try:
            super(Feed, self).save(*args, **kwargs)
        except IntegrityError, e:
            logging.debug(" ---> ~FRFeed save collision (%s), checking dupe..." % e)
            duplicate_feeds = Feed.objects.filter(
                                        feed_address=self.feed_address,
                                        feed_link=self.feed_link)
            if not duplicate_feeds:
                feed_address = self.feed_address or ""
                feed_link = self.feed_link or ""
                hash_address_and_link = hashlib.sha1(feed_address + feed_link).hexdigest()
                duplicate_feeds = Feed.objects.filter(hash_address_and_link=hash_address_and_link)
            if not duplicate_feeds:
                # Feed has been deleted. Just ignore it.
                logging.debug(" ***> Changed to: %s - %s: %s" %
                              (self.feed_address, self.feed_link, duplicate_feeds))
                logging.debug(
                    ' ***> [%-30s] Feed deleted (%s).' % (unicode(self)[:30], self.pk))
                return

            if self.pk != duplicate_feeds[0].pk:
                logging.debug(
                    " ---> ~FRFound different feed (%s), merging..." % duplicate_feeds[0])
                # feed = Feed.get_by_id(merge_feeds(duplicate_feeds[0].pk, self.pk))
                return feed

        return self

    def index_for_search(self):
            SearchFeed.index(feed_id=self.pk, title=self.feed_title,
                             address=self.feed_address, link=self.feed_link)

    @classmethod
    def get_by_id(cls, feed_id, feed_address=None):
        try:
            feed = Feed.objects.get(pk=feed_id)
            return feed
        except Feed.DoesNotExist:
            raise Feed.DoesNotExist

    @classmethod
    def get_by_name(cls, query, limit=1):
        results = SearchFeed.query(query)
        feed_ids = [result.feed_id for result in results]

        if limit == 1:
            return Feed.get_by_id(feed_ids[0])
        else:
            return [Feed.get_by_id(f) for f in feed_ids][:limit]

    def fix_google_alerts_urls(self):
        if (self.feed_address.startswith('http://user/') and
            '/state/com.google/alerts/' in self.feed_address):
            match = re.match(
                r"http://user/(\d+)/state/com.google/alerts/(\d+)", self.feed_address)
            if match:
                user_id, alert_id = match.groups()
                self.feed_address = "http://www.google.com/alerts/feeds/%s/%s" % (
                    user_id, alert_id)

    # put self.pk into scheduled_updates immediately.
    def schedule_feed_fetch_immediately(self, verbose=True):
        r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)
        if verbose:
            logging.debug(
                '   ---> [%-30s] Scheduling feed fetch immediately...' % (unicode(self)[:30]))

        self.next_scheduled_update = datetime.datetime.utcnow()
        r.zadd('scheduled_updates', self.pk,
               self.next_scheduled_update.strftime('%s'))

        return self.save()


    @classmethod
    def task_feeds(cls, feeds, queue_size=12, verbose=True):
        if not feeds:
            return
        r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)

        if isinstance(feeds, Feed):
            if verbose:
                logging.debug(" ---> ~SN~FBTasking feed: ~SB%s" % feeds)
            feeds = [feeds.pk]
        elif verbose:
            logging.debug(" ---> ~SN~FBTasking ~SB%s~SN feeds..." % len(feeds))

        if isinstance(feeds, QuerySet):
            feeds = [f.pk for f in feeds]

        # delete feed_id from queued_feeds, and put into tasked_feeds, and package tasks.
        r.srem('queued_feeds', *feeds)
        now = datetime.datetime.now().strftime("%s")
        p = r.pipeline()
        for feed_id in feeds:
            p.zadd('tasked_feeds', feed_id, now)
        p.execute()

        for feed_id in feeds:
            UpdateFeeds.apply_async(args=(feed_id,), queue='update_feeds')


    def update(self, **kwargs):
        from utils import feed_fetcher
        r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)
        original_feed_id = int(self.pk)

        options = {
            'verbose': kwargs.get('verbose', False),
            'timeout': 10,
            'single_threaded': kwargs.get('single_threaded', True),
            'force': kwargs.get('force'),
            'fake': kwargs.get('fake'),
            'quick': kwargs.get('quick'),
            'debug': kwargs.get('debug'),
        }
        disp = feed_fetcher.Dispatcher(options, 1)
        disp.add_jobs([[self.pk]])
        feed = disp.run_jobs()

        if feed:
            feed = Feed.get_by_id(feed.pk)
        if feed:
            # After the fetch task is done, update last_update and set next_scheduled_update
            feed.last_update = datetime.datetime.utcnow()
            feed.set_next_scheduled_update()
            # Then add the feed_id into fetched_feeds_last_hour(Sort_Set), means fetch it in a last hour.
            r.zadd('fetched_feeds_last_hour', feed.pk,
                   int(datetime.datetime.now().strftime('%s')))

        if not feed or original_feed_id != feed.pk:
            logging.info(" ---> ~FRFeed changed id, removing %s from tasked_feeds queue..." %
                original_feed_id)
            r.zrem('tasked_feeds', original_feed_id)
            r.zrem('error_feeds', original_feed_id)
        # If task is done, then delete it from tasked_feeds and error_feeds.
        if feed:
            r.zrem('tasked_feeds', feed.pk)
            r.zrem('error_feeds', feed.pk)

        return feed

    # for the feed_id, update last_story_date and stories_last_month
    def update_all_statistics(self, full=True, force=False):
        self.calculate_last_story_date()

        if force or full:
            self.save_feed_stories_last_month()

    def calculate_last_story_date(self):
        last_story_date = None

        try:
            latest_story = MStory.objects(story_feed_id=self.pk).limit(1).order_by(
                '-story_date').only('story_date').first()
            if latest_story:
                last_story_date = latest_story.story_date
        except MStory.DoesNotExist:
            pass

        if not last_story_date or seconds_timesince(last_story_date) < 0:
            last_story_date = datetime.datetime.now()

        self.last_story_date = last_story_date
        self.save()

    # count the number of stories last month
    def save_feed_stories_last_month(self, verbose=False):
        month_ago = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        stories_last_month = MStory.objects(story_feed_id=self.pk,
                                            story_date__gte=month_ago).count()
        self.stories_last_month = stories_last_month

        self.save()

        if verbose:
            print "  ---> %s [%s]: %s stories last month" % (self.feed_title, self.pk,
                                                             self.stories_last_month)

    # get feed_address from feed_link, merge_feeds!!!
    def check_feed_link_for_feed_address(self):
        @timelimit(10)
        def _1():
            feed_address = None
            feed = self
            try:
                is_feed = feedfinder.isFeed(self.feed_address)
            except KeyError:
                is_feed = False
            if not is_feed:
                feed_address = feedfinder.feed(self.feed_address)
                if not feed_address and self.feed_link:
                    feed_address = feedfinder.feed(self.feed_link)
            else:
                feed_address_from_link = feedfinder.feed(self.feed_link)
                if feed_address_from_link != self.feed_address:
                    feed_address = feed_address_from_link

            if feed_address:
                if (feed_address.endswith('feedburner.com/atom.xml') or
                    feed_address.endswith('feedburner.com/feed/')):
                    logging.debug("  ---> Feed points to 'Wierdo', ignoring.")
                    return False, self
                try:
                    self.feed_address = feed_address
                    feed = self.save()
                    feed.schedule_feed_fetch_immediately()
                    feed.has_feed_exception = False
                    feed = feed.save()
                except IntegrityError:
                    original_feed = Feed.objects.get(
                        feed_address=feed_address, feed_link=self.feed_link)
                    original_feed.has_feed_exception = False
                    original_feed.save()
                    merge_feeds(original_feed.pk, self.pk)
            return feed_address, feed

        try:
            feed_address, feed = _1()
        except TimeoutError, e:
            logging.debug(
                '   ---> [%-30s] Feed address check timed out...' % (unicode(self)[:30]))
            self.save_feed_history(505, 'Timeout', e)
            feed = self
            feed_address = None

        return bool(feed_address), feed

    def get_next_scheduled_update(self, force=False, verbose=True):
        if self.min_to_decay and not force:
            return self.min_to_decay

        upd = self.stories_last_month / 30.0        # average stories each day.
        # subs = (self.active_premium_subscribers +
        #         ((self.active_subscribers - self.active_premium_subscribers) / 10.0))
        # UPD = 1  Subs > 1:  t = 5         # 11625  * 1440/5 =       3348000
        # UPD = 1  Subs = 1:  t = 60        # 17231  * 1440/60 =      413544
        # UPD < 1  Subs > 1:  t = 60        # 37904  * 1440/60 =      909696
        # UPD < 1  Subs = 1:  t = 60 * 12   # 143012 * 1440/(60*12) = 286024
        # UPD = 0  Subs > 1:  t = 60 * 3    # 28351  * 1440/(60*3) =  226808
        # UPD = 0  Subs = 1:  t = 60 * 24   # 807690 * 1440/(60*24) = 807690

        if upd >= 1:    
            total = 10
        elif upd > 0:
            total = 60 - (upd * 60)
        elif upd == 0:      # If the last month has no stroies.
            total = 60 * 6
            months_since_last_story = seconds_timesince(self.last_story_date) / (60 * 60 * 24 * 30)
            total *= max(1, months_since_last_story)

        # if self.is_push:
        #     fetch_history = MFetchHistory.feed(self.pk)
        #     if len(fetch_history['push_history']):
        #         total = total * 12

        # 3 day max
        total = min(total, 60 * 24 * 2)

        if verbose:
            logging.debug("   ---> [%-30s] Fetched every %s min - Stories: %s" % (
                unicode(self)[:30], total, upd))
        return total            # Note: fetched every total min

    # set next scheduled update, and put (feed_id, next_scheduled_update) into scheduled_updates(SortedSet)
    def set_next_scheduled_update(self, verbose=False, skip_scheduling=False):
        r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)
        total = self.get_next_scheduled_update(force=True, verbose=verbose)
        error_count = self.error_count

        # look up how many errors, and multiply total, fetch geometrically.
        if error_count:
            total = total * error_count
            total = min(total, 60 * 24 * 7) # no more than 7 days
            if verbose:
                logging.debug('   ---> [%-30s] ~FBScheduling feed fetch geometrically: '
                              '~SB%s errors. Time: %s min' % (
                              unicode(self)[:30], self.errors_since_good, total))

        random_factor = random.randint(0, total) / 4 + 5
        # next_scheduled_update is now + total + random_factor
        next_scheduled_update = datetime.datetime.utcnow() + datetime.timedelta(
            minutes=total + random_factor)

        self.min_to_decay = total

        delta = self.next_scheduled_update - datetime.datetime.now()

        # !!!!!
        #minutes_to_next_fetch = delta.total_seconds() / 60 
        minutes_to_next_fetch = (delta.seconds + (delta.days * 24 * 3600)) / 60
        if minutes_to_next_fetch > self.min_to_decay or not skip_scheduling:
            self.next_scheduled_update = next_scheduled_update
            r.zadd('scheduled_updates', self.pk, self.next_scheduled_update.strftime('%s'))
            r.zrem('tasked_feeds', self.pk)
            r.srem('queued_feeds', self.pk)

        self.save()


    def save_feed_history(self, status_code, message, exception=None):
        # Note: the status_code, message, feed_fetch_type will be added to history and return.
        fetch_history = MFetchHistory.add(feed_id=self.pk,
                                          fetch_type='feed',
                                          code=int(status_code),
                                          message=message,
                                          exception=exception)

        # if the status_code is not 200 or 304, it means it is wrong
        if status_code not in (200, 304):
            self.errors_since_good += 1
            self.count_errors_in_history('feed', status_code, fetch_history=fetch_history)
            self.set_next_scheduled_update()
        elif self.has_feed_exception or self.errors_since_good:
            self.errors_since_good = 0
            self.has_feed_exception = False
            self.save()

    def count_errors_in_history(self, exception_type='feed', status_code=None, fetch_history=None):
        logging.debug('   ---> [%-30s] Counting errors in history...' % (unicode(self)[:30]))

        if not fetch_history:
            fetch_history = MFetchHistory.feed(self.pk)
        fh = fetch_history[exception_type + '_fetch_history']
        non_errors = [h for h in fh if h['status_code'] and int(h['status_code'])     in (200, 304)]
        errors     = [h for h in fh if h['status_code'] and int(h['status_code']) not in (200, 304)]

        if len(non_errors) == 0 and len(errors) > 1:
            if exception_type == 'feed':
                self.has_feed_exception = True
            self.exception_code = status_code or int(errors[0])
            self.save()
        # !!!!!
        elif self.exception_code > 0:
            self.exception_code = 0
            if exception_type == 'feed':
                self.has_feed_exception = False
            self.save()

        return errors, non_errors

    # stories is a list of story which is in feed_address html
    # existing_stories is dict {story_hash: story instance}
    def add_update_stories(self, stories, existing_stories, verbose=False):
        ret_values = dict(new=0, updated=0, same=0, error=0)
        error_count = self.error_count
        new_story_hashes = [s.get('story_hash') for s in stories]

        if settings.DEBUG or verbose:
            logging.debug("   ---> [%-30s] ~FBChecking ~SB%s~SN new/updated against ~SB%s~SN stories" % (
                          self.title[:30],
                          len(stories),
                          len(existing_stories.keys())))

        @timelimit(2)
        def _1(story, story_content, existing_stories):
            existing_story, story_has_changed = self._exists_story(story, story_content, 
                                                                    existing_stories, new_story_hashes)
            return existing_story, story_has_changed

        # for each story in feed_address html
        for story in stories:
            if verbose:
                logging.debug("   ---> [%-30s] ~FBChecking ~SB%s~SN / ~SB%s" % (
                              self.title[:30],
                              story.get('title'),
                              story.get('guid')))
            if not story.get('title'):
                continue

            story_content = story.get('story_content')
            if error_count:
                story_content = strip_comments__lxml(story_content)
            else:
                story_content = strip_comments(story_content)   # remove the comments
            story_tags = self.get_tags(story)                   # get the tags of the story
            story_link = self.get_permalink(story)              # get the link of the story

            try:
                existing_story, story_has_changed = _1(story, story_content, existing_stories)
            except TimeoutError, e:
                logging.debug('   ---> [%-30s] ~SB~FRExisting story check timed out...' % (unicode(self)[:30]))
                existing_story = None
                story_has_changed = False

            # If the story is new.
            if existing_story is None:
                if settings.DEBUG and False:
                    logging.debug('   ---> New story in feed (%s - %s): %s' %
                                  (self.feed_title, story.get('title'), len(story_content)))

                s = MStory(story_feed_id=self.pk,
                           story_date=story.get('published'),
                           story_title=story.get('title'),
                           story_content=story_content,
                           story_author_name=story.get('author'),
                           story_permalink=story_link,
                           story_guid=story.get('guid'),
                           story_tags=story_tags
                           )
                s.extract_image_urls()

                try:
                    s.save()
                    ret_values['new'] += 1
                    #=========================
                    #=Added by SongJun:      =
                    #==fetch_original_text   =
                    #=========================
                    original_text = s.fetch_original_text()
                    soup = BeautifulSoup(str(original_text))
                    imgs = soup.findAll('img')
                    for img in imgs:
                        imgurl = img.get('src')
                        if not imgurl: continue
                        if imgurl and len(imgurl) >= 1024:
                            continue
                        if not(imgurl in s.image_urls):
                            s.image_urls.append(imgurl)
                    #=========================
                except (IntegrityError, OperationError), e:
                    ret_values['error'] += 1
                    if settings.DEBUG:
                        logging.info('   ---> [%-30s] ~SN~FRIntegrityError on new story: %s - %s' % (
                            self.feed_title[:30], story.get('guid'), e))

            # If the story is existing, and update it.
            elif existing_story and story_has_changed:
                original_content = None
                try:
                    if existing_story and existing_story.id: # id is ObjectId
                        try:
                            existing_story = MStory.objects.get(id=existing_story.id)
                        except ValidationError:
                            existing_story, _ = MStory.find_story(existing_story.story_feed_id,
                                                                    existing_story.id)
                    elif existing_story and existing_story.story_guid:
                        existing_story, _ = MStory.find_story(existing_story.story_feed_id,
                                                                existing_story.story_guid)
                    else:
                        raise MStory.DoesNotExist
                except (MStory.DoesNotExist, OperationError), e:
                    ret_values['error'] += 1
                    if verbose:
                        logging.info('   ---> [%-30s] ~SN~FROperation on existing story: %s - %s' % (
                            self.feed_title[:30], story.get('guid'), e))
                    continue

                if story_content and len(story_content) > 10:
                    story_content_diff = htmldiff(unicode(original_content), unicode(story_content))
                else:
                    story_content_diff = original_content
                if existing_story.story_guid != story.get('guid'):
                    self.update_story_with_new_guid(existing_story, story.get('guid'))

                if settings.DEBUG and False:
                    logging.debug('- Updated story in feed (%s - %s): %s / %s' %
                                (self.feed_title, story.get('title'), len(story_content_diff), len(story_content)))

                existing_story.story_feed = self.pk
                existing_story.story_title = story.get('title')

                existing_story.story_content = story_content_diff

                existing_story.story_author_name = story.get('author')
                existing_story.story_permalink = story_link
                existing_story.story_guid = story.get('guid')
                existing_story.story_tags = story_tags
                existing_story.extract_image_urls()

                try:
                    existing_story.save()
                    ret_values['updated'] += 1
                except (IntegrityError, OperationError):
                    ret_values['error'] += 1
                    if verbose:
                        logging.info('   ---> [%-30s] ~SN~FRIntegrityError on updated story: %s' % (
                            self.feed_title[:30], story.get('title')[:30]))
                except ValidationError:
                    ret_values['error'] += 1
                    if verbose:
                        logging.info('   ---> [%-30s] ~SN~FRValidationError on updated story: %s' % (
                            self.feed_title[:30], story.get('title')[:30]))
            else:
                ret_values['same'] += 1

        return ret_values

    # a story, and the stroy_content
    # existing_stories is including stories dict, like the form {story hash: story instance}
    def _exists_story(self, story, story_content, existing_stories, new_story_hashes):
        story_in_system = None
        story_has_changed = False
        story_link = self.get_permalink(story)
        existing_stories_hashes = existing_stories.keys()
        story_pub_date = story.get('published')

        for existing_story in existing_stories.values():
            content_ratio = 0

            if isinstance(existing_story.id, unicode):
                existing_story.story_guid = existing_story.id
            
            if story.get('story_hash') == existing_story.story_hash:
                story_in_system = existing_story
            elif (story.get('story_hash') in existing_stories_hashes and 
                story.get('story_hash') != existing_story.story_hash):
                # Story already exists but is not this one
                continue
            elif (existing_story.story_hash in new_story_hashes and
                  story.get('story_hash') != existing_story.story_hash):
                  # Story coming up later
                continue

            if 'story_content_z' in existing_story:
                existing_story_content = unicode(zlib.decompress(existing_story.story_content_z))
            elif 'story_content' in existing_story:
                existing_story_content = existing_story.story_content
            else:
                existing_story_content = u''
                
                  
            # Title distance + content distance, checking if story changed
            story_title_difference = abs(levenshtein_distance(story.get('title'),
                                                              existing_story.story_title))
            
            title_ratio = difflib.SequenceMatcher(None, story.get('title', ""),
                                                  existing_story.story_title).ratio()
            if title_ratio < .75: continue
            
            story_timedelta = existing_story.story_date - story_pub_date
            if abs(story_timedelta.days) >= 1: continue
            
            seq = difflib.SequenceMatcher(None, story_content, existing_story_content)
            
            similiar_length_min = 1000
            if (existing_story.story_permalink == story_link and 
                existing_story.story_title == story.get('title')):
                similiar_length_min = 20
            
            if (seq
                and story_content
                and len(story_content) > similiar_length_min
                and existing_story_content
                and seq.real_quick_ratio() > .9 
                and seq.quick_ratio() > .95):
                content_ratio = seq.ratio()
                
            if story_title_difference > 0 and content_ratio > .98:
                story_in_system = existing_story
                if story_title_difference > 0 or content_ratio < 1.0:
                    if settings.DEBUG:
                        logging.debug(" ---> Title difference - %s/%s (%s): %s" % 
                            (story.get('title'), existing_story.story_title, story_title_difference, content_ratio))
                    story_has_changed = True
                    break
            
            # More restrictive content distance, still no story match
            if not story_in_system and content_ratio > .98:
                if settings.DEBUG:
                    logging.debug(" ---> Content difference - %s/%s (%s): %s" % 
                        (story.get('title'), existing_story.story_title, story_title_difference, content_ratio))
                story_in_system = existing_story
                story_has_changed = True
                break
                
            if story_in_system and not story_has_changed:
                if story_content != existing_story_content:
                    if settings.DEBUG:
                        logging.debug(" ---> Content difference - %s (%s)/%s (%s)" % 
                            (story.get('title'), len(story_content), existing_story.story_title, len(existing_story_content)))
                    story_has_changed = True
                if story_link != existing_story.story_permalink:
                    if settings.DEBUG:
                        logging.debug(" ---> Permalink difference - %s/%s" % 
                            (story_link, existing_story.story_permalink))
                    story_has_changed = True
                # if story_pub_date != existing_story.story_date:
                #     story_has_changed = True
                break
                
        
        # if story_has_changed or not story_in_system:
        #     print 'New/updated story: %s' % (story), 
        return story_in_system, story_has_changed


    def update_story_with_new_guid(self, existing_story, new_story_guid):

        new_hash = MStory.ensure_story_hash(new_story_guid, self.pk)

    def freeze_feed(self,verbose=True):
        MStory.freeze_feed(feed=self,verbose=verbose)

    def get_stories(self, offset=0, limit=25, force=False):
        stories_db = MStory.objects(story_feed_id=self.pk)[offset:offset + limit]
        stories = self.format_stories(stories_db, self.pk)

        return stories

    @classmethod
    def find_feed_stories(cls, feed_ids, query, offset=0, limit=25):
        stories_db = MStory.objects(
            Q(story_feed_id__in=feed_ids) &
            (Q(story_title__icontains=query) |
             Q(story_author_name__icontains=query) |
             Q(story_tags__icontains=query))
        ).order_by('-story_date')[offset:offset + limit]
        stories = cls.format_stories(stories_db)

        return stories

    def find_stories(self, query, offset=0, limit=25):
        stories_db = MStory.objects(
            Q(story_feed_id=self.pk) &
            (Q(story_title__icontains=query) |
             Q(story_author_name__icontains=query) |
             Q(story_tags__icontains=query))
        ).order_by('-story_date')[offset:offset + limit]
        stories = self.format_stories(stories_db, self.pk)

        return stories

    @classmethod
    def format_stories(cls, stories_db, feed_id=None, include_permalinks=False):
        stories = []

        for story_db in stories_db:
            story = cls.format_story(
                story_db, feed_id, include_permalinks=include_permalinks)
            stories.append(story)

        return stories

    @classmethod
    def format_story(cls, story_db, feed_id=None, text=False, include_permalinks=False):
        if isinstance(story_db.story_content_z, unicode):
            story_db.story_content_z = story_db.story_content_z.decode(
                'base64')

        story_content = story_db.story_content_z and zlib.decompress(
            story_db.story_content_z) or ''
        story = {}
        story['story_hash'] = getattr(story_db, 'story_hash', None)
        story['story_tags'] = story_db.story_tags or []
        story['story_date'] = story_db.story_date.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(settings.TIME_ZONE))
        story['story_timestamp'] = story_db.story_date.strftime('%s')
        story['story_authors'] = story_db.story_author_name
        story['story_title'] = story_db.story_title
        story['story_content'] = story_content
        story['story_permalink'] = story_db.story_permalink
        story['image_urls'] = story_db.image_urls
        story['story_feed_id'] = feed_id or story_db.story_feed_id
        story['guid_hash'] = story_db.guid_hash if hasattr(story_db, 'guid_hash') else None
        if hasattr(story_db, 'source_user_id'):
            story['source_user_id'] = story_db.source_user_id
        story['id'] = story_db.story_guid or story_db.story_date
        if hasattr(story_db, 'starred_date'):
            story['starred_date'] = story_db.starred_date
        if hasattr(story_db, 'shared_date'):
            story['shared_date'] = story_db.shared_date
        if include_permalinks and hasattr(story_db, 'blurblog_permalink'):
            story['blurblog_permalink'] = story_db.blurblog_permalink()
        if text:
            soup = BeautifulSoup(story['story_content'])
            text = ''.join(soup.findAll(text=True))
            text = re.sub(r'\n+', '\n\n', text)
            text = re.sub(r'\t+', '\t', text)
            story['text'] = text
        if '<ins' in story['story_content'] or '<del' in story['story_content']:
            story['has_modifications'] = True

        return story

    # get the details of the categories for the entry
    def get_tags(self, entry):
        fcat = []
        if entry.has_key('tags'):
            for tcat in entry.tags:
                term = None
                if hasattr(tcat, 'label') and tcat.label:
                    term = tcat.label
                elif hasattr(tcat, 'term') and tcat.term:
                    term = tcat.term
                if not term:
                    continue
                qcat = term.strip()
                if ',' in qcat or '/' in qcat:
                    qcat = qcat.replace(',', '/').split('/')
                else:
                    qcat = [qcat]
                for zcat in qcat:
                    tagname = zcat.lower()
                    while '  ' in tagname:
                        tagname = tagname.replace('  ', ' ')
                    tagname = tagname.strip()
                    if not tagname or tagname == ' ':
                        continue
                    fcat.append(tagname)
        fcat = [strip_tags(t)[:250] for t in fcat[:12]]
        return fcat

    @classmethod
    def get_permalink(self, entry):
        link = entry.get('link')
        if not link:
            links = entry.get('links')
            if links:
                link = links[0].get('href')
        if not link:
            link = entry.get('id')
        return link

class MStory(mongo.Document):

    '''A feed item'''
    story_feed_id = mongo.IntField()                    # which feed the story belongs to
    story_date = mongo.DateTimeField()
    story_title = mongo.StringField(max_length=1024)

    story_content = mongo.StringField()       
    story_content_z = mongo.BinaryField()

    original_text_z = mongo.BinaryField()               # the main body of story_permalink, and it's from 
                                                        # fetch_original_text and text_importer.py
    original_page_z = mongo.BinaryField()               # the html of story_permalink

    story_author_name = mongo.StringField()
    story_permalink = mongo.StringField()
    story_guid = mongo.StringField()
    story_hash = mongo.StringField()                    # feed_id:hashlib.sha1(self.story_guid).hexdigest()[:6]
    image_urls = mongo.ListField(mongo.StringField(max_length=1024)) # the list of image urls from story_content

    image_ids = mongo.ListField(mongo.StringField())
    story_tags = mongo.ListField(mongo.StringField(max_length=250))

    meta = {
        'collection': 'stories',
        'indexes': [('story_feed_id', '-story_date'),
                    {'fields': ['story_hash'],
                     'unique': True,
                     'types': False, }],
        'index_drop_dups': True,
        'ordering': ['-story_date'],
        'allow_inheritance': False,
        'cascade': False,
    }

    RE_STORY_HASH = re.compile(r"^(\d{1,10}):(\w{6})$")
    RE_RS_KEY = re.compile(r"^RS:(\d+):(\d+)$")

    @property
    def story_tidy_content(self):
        content = self.story_original_content_uz
        if len(self.story_content_uz) > len(content):
            content = self.story_content_uz
        # replace images using local images
        bsoup = BeautifulSoup(str(content))
        for image in bsoup.findAll('img'):
            remote_url = image['src']
            stored_image = MImage.get_by_url(remote_url)
            if stored_image:
                image['src'] = settings.FDFS_HTTP_SERVER + stored_image.image_remote_id
        return str(bsoup)

    @property
    def story_content_uz(self):
        if self.story_content == None:
            self.story_content = zlib.decompress(self.story_content_z)
        return self.story_content

    @property
    def story_original_content_uz(self):
        if self.original_text_z:
            return zlib.decompress(self.original_text_z)
        else:
            return ''

    @classmethod
    def guid_hash_unsaved(self, guid):
        return hashlib.sha1(guid).hexdigest()[:6]

    @property
    def guid_hash(self):
        return hashlib.sha1(self.story_guid).hexdigest()[:6]

    @property
    def feed_guid_hash(self):
        return "%s:%s" % (self.story_feed_id, self.guid_hash)

    @classmethod
    def feed_guid_hash_unsaved(cls, feed_id, guid):
        return "%s:%s" % (feed_id, cls.guid_hash_unsaved(guid))

    def save(self, *args, **kwargs):
        story_title_max = MStory._fields['story_title'].max_length
        self.story_hash = self.feed_guid_hash

        # store original story_content, or it will be erased
        story_content = self.story_content

        if self.story_content:
            self.story_content_z = zlib.compress(self.story_content)
            self.story_content = None
        if self.story_title and len(self.story_title) > story_title_max:
            self.story_title = self.story_title[:story_title_max]

        super(MStory, self).save(*args, **kwargs)

        #Add by Xinyan Lu : index on save
        if not story_content and self.story_content_z:
            story_content = zlib.decompress(self.story_content_z)
        # SearchStory.index(story_id=self.story_guid,
        #                     story_title=self.story_title,
        #                     story_content=story_content,
        #                     story_author=self.story_author_name,
        #                     story_date=self.story_date,
        #                     db_id=str(self.id))

        return self

    def delete(self, *args, **kwargs):
        super(MStory, self).delete(*args, **kwargs)

    #Modified By Xinyan Lu : support for MFrozenStory
    @classmethod
    def freeze_feed(cls, feed_id=None, feed=None, verbose=True):
        extra_stories_count = 0
        if not feed_id and not feed:
            return extra_stories_count

        if not feed_id:
            feed_id = feed.pk
        if not feed:
            feed = feed_id

        cutoff = 100
        week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=14)
        stories = cls.objects(story_feed_id=feed_id).order_by('-story_date')
        if stories.count() <= cutoff:
            return 0
        
        latest_week_stories = stories.filter(story_date__gte=week_ago)
        if latest_week_stories.count() > cutoff:
            extra_stories = stories.filter(story_date__lte=week_ago)
        else:
            extra_stories = stories[cutoff:]
        logging.debug(
                '   ---> [%-30s] ~FMFound %s stories. Frozen ~SB%s~SN...' %
                (unicode(feed)[:30], stories.count(), extra_stories.count()))

        frozen_num = 0
        for story in extra_stories:
            frozen_story = MFrozenStory(story)
            frozen_story.save()
            story.delete()
            frozen_num += 1
            if frozen_num % 10 == 0:
                print >>sys.stderr, '.',
        print >>sys.stderr, '.'
        logging.debug("   ---> Frozen %d stories." % (frozen_num,))
        # if verbose:
        #     existing_story_count = MStory.objects(story_feed_id=feed_id).count()
        #     logging.debug("   ---> Deleted %s stories, %s left." % (
        #         extra_stories_count, existing_story_count))

        return frozen_num

    @classmethod
    def find_story(cls, story_feed_id, story_id):
        original_found = False
        story_hash = cls.ensure_story_hash(story_id, story_feed_id)

        if isinstance(story_id, ObjectId):
            story = cls.objects(id=story_id).limit(1).first()
        else:
            story = cls.objects(story_hash=story_hash).limit(1).first()

        if story:
            original_found = True

        return story, original_found


    @classmethod
    def ensure_story_hash(cls, story_id, story_feed_id):
        if not cls.RE_STORY_HASH.match(story_id):
            story_id = "%s:%s" % (
                story_feed_id, hashlib.sha1(story_id).hexdigest()[:6])

        return story_id

    @classmethod
    def split_story_hash(cls, story_hash):
        matches = cls.RE_STORY_HASH.match(story_hash)
        if matches:
            groups = matches.groups()
            return groups[0], groups[1]
        return None, None

    @classmethod
    def split_rs_key(cls, rs_key):
        matches = cls.RE_RS_KEY.match(rs_key)
        if matches:
            groups = matches.groups()
            return groups[0], groups[1]
        return None, None

    @classmethod
    def story_hashes(cls, story_ids):
        story_hashes = []
        for story_id in story_ids:
            story_hash = cls.ensure_story_hash(story_id)
            if not story_hash:
                continue
            story_hashes.append(story_hash)

        return story_hashes

    def extract_image_urls(self, force=False):
        '''
        extract images from story_content
        '''
        if self.image_urls and not force:
            return self.image_urls

        story_content = self.story_content
        if not story_content and self.story_content_z:
            story_content = zlib.decompress(self.story_content_z)
        if not story_content:
            return

        try:
            soup = BeautifulSoup(story_content)
        except ValueError:
            return

        images = soup.findAll('img')
        if not images: return

        image_urls = []
        for image in images:
            image_url = image.get('src')
            if not image_url:
                continue
            if image_url and len(image_url) >= 1024:
                continue
            image_urls.append(image_url)

        if not image_urls: return

        self.image_urls = image_urls
        return self.image_urls

    def fetch_original_text(self, force=False, request=None):
        '''
        according to the story.story_permalink,
        this method go to fetch the html document and get the content
        If original_text_z is not null, the decompress it and return.
        '''
        original_text_z = self.original_text_z

        if not original_text_z or force:
                ti = TextImporter(self, request=request)
                original_text = ti.fetch()
        else:
            logging.user(request, "~FYFetching ~FGoriginal~FY story text, ~SBfound.")
            original_text = zlib.decompress(original_text_z)

        return original_text


class MFrozenStory(mongo.Document):

    '''A feed item (Frozen)'''
    story_feed_id = mongo.IntField()
    story_date = mongo.DateTimeField()
    story_title = mongo.StringField(max_length=1024)
    story_content = mongo.StringField()
    story_content_z = mongo.BinaryField()
    original_text_z = mongo.BinaryField()
    story_author_name = mongo.StringField()
    story_permalink = mongo.StringField()
    story_guid = mongo.StringField()
    story_hash = mongo.StringField()
    image_urls = mongo.ListField(mongo.StringField(max_length=1024))
    # Add by Xinyan Lu : support images storage
    image_ids = mongo.ListField(mongo.StringField())
    story_tags = mongo.ListField(mongo.StringField(max_length=250))

    meta = {
        'collection': 'stories_frozen',
        'indexes': [('story_feed_id', '-story_date'),
                    {'fields': ['story_hash'],
                     'unique': True,
                     'types': False, }],
        'index_drop_dups': True,
        'ordering': ['-story_date'],
        'allow_inheritance': False,
        'cascade': False,
    }

    def __init__(self, mstory):
        super(MFrozenStory,self).__init__()
        if not isinstance(mstory,MStory):
            raise TypeError('Not a MStory object')
        if not mstory.id:
            raise AssertionError('To move to new collection, an ObjectID is needed.')
        self.story_feed_id = mstory.story_feed_id
        self.story_date = mstory.story_date
        self.story_title = mstory.story_title
        self.story_content = mstory.story_content
        self.story_content_z = mstory.story_content_z
        self.original_text_z = mstory.original_text_z
        self.story_author_name = mstory.story_author_name
        self.story_permalink = mstory.story_permalink
        self.story_guid = mstory.story_guid
        self.story_hash = mstory.story_hash
        self.image_urls = mstory.image_urls
        self.image_ids = mstory.image_ids
        self.story_tags = mstory.story_tags

        # save with the same mongodb ObjectID
        self.id = mstory.id


    def save(self, *args, **kwargs):
        # store original story_content, or it will be erased
        story_content = self.story_content

        if self.story_content:
            self.story_content_z = zlib.compress(self.story_content)
            self.story_content = None
        # if self.story_title and len(self.story_title) > story_title_max:
        #     self.story_title = self.story_title[:story_title_max]
        super(MFrozenStory, self).save(*args, **kwargs)

        #Add by Xinyan Lu : index on save
        if not story_content and self.story_content_z:
            try:
                story_content = zlib.decompress(self.story_content_z)
            except Exception,e:
                story_content = ''
                pass
        index_content = story_content or ''
        if self.original_text_z:
            try:
                original_text = zlib.decompress(self.original_text_z)
            except Exception,e:
                original_text = ''
            if len(original_text) > 1.5 * len(index_content):
                index_content = original_text

        SearchStory.index(story_id=self.story_guid,
                            story_title=self.story_title,
                            story_content=index_content,
                            story_author=self.story_author_name,
                            story_date=self.story_date,
                            db_id=str(self.id),
                            frozen=True)

        return self



class MFetchHistory(mongo.Document):

    # for each feed_id, there is two fields.

    feed_id = mongo.IntField(unique=True)
    feed_fetch_history = mongo.DynamicField()
    page_fetch_history = mongo.DynamicField()

    meta = {
        'db_alias': 'doctopus',
        'collection': 'fetch_history',
        'allow_inheritance': False,
    }

    @classmethod
    def feed(cls, feed_id, timezone=None, fetch_history=None):
        if not fetch_history:
            try:
                fetch_history = cls.objects.read_preference(pymongo.ReadPreference.PRIMARY)\
                                           .get(feed_id=feed_id)
            except cls.DoesNotExist:
                fetch_history = cls.objects.create(feed_id=feed_id)
        history = {}

        for fetch_type in ['feed_fetch_history', 'page_fetch_history']:
            history[fetch_type] = getattr(fetch_history, fetch_type) # similar to fetch_history.fetch_type
            if not history[fetch_type]:
                history[fetch_type] = []
            for f, fetch in enumerate(history[fetch_type]):
                date_key = 'fetch_date'
                history[fetch_type][f] = {
                    date_key: localtime_for_timezone(fetch[0],
                                                     timezone).strftime(
                                                         "%Y-%m-%d %H:%M:%S"),
                    'status_code': fetch[1],
                    'message': fetch[2]
                }
        return history

    # Get the fetch and push history for a specific feed_id.
    # and then add [[date, code, message]] into the list of fetch_type
    @classmethod
    def add(cls, feed_id, fetch_type, date=None, message=None, code=None, exception=None):
        if not date:
            date = datetime.datetime.now()
        try:
            fetch_history = cls.objects.read_preference(pymongo.ReadPreference.PRIMARY)\
                                       .get(feed_id=feed_id)
        except cls.DoesNotExist:
            fetch_history = cls.objects.create(feed_id=feed_id)

        if fetch_type == 'feed':
            history = fetch_history.feed_fetch_history or []
        elif fetch_type == 'page':
            history = fetch_history.page_fetch_history or []

        history = [[date, code, message]] + history
        if code and code >= 400:
            history = history[:50]
        else:
            history = history[:5]

        if fetch_type == 'feed':
            fetch_history.feed_fetch_history = history
        elif fetch_type == 'page':
            fetch_history.page_fetch_history = history

        fetch_history.save()

        return cls.feed(feed_id, fetch_history=fetch_history)


class MImage(mongo.Document):

    '''An Image item'''
    image_guid = mongo.StringField(required=True) #the URL
    image_hash = mongo.StringField(required=True)
    image_size = mongo.IntField()
    image_type = mongo.StringField()
    image_width = mongo.IntField()
    image_height = mongo.IntField()
    image_remote_id = mongo.StringField()
    story_ids = mongo.ListField(mongo.StringField(),required=True)
    image_has_error = mongo.BooleanField(required=True,default=False)
    image_error_code = mongo.StringField()

    last_reference_date = mongo.DateTimeField()
    # store the image buffer
    image_buffer = None


    meta = {
        'collection': 'images',
        'indexes': ['image_hash','last_reference_date'],
        'index_drop_dups': False,
        'allow_inheritance': False,
        'cascade': False,
    }

    def process(self, url, story, client=None, verbose=False):
        if self.exists_image(url) == True:
            raise IntegrityError('Image has been in the datatabase')

        self.image_guid = url
        self.image_hash = self.guid_hash
        self.story_ids = [str(story.id)]
        self.image_has_error = False
        try:
            self.image_buffer = self._fetch_image(story_url=story.story_guid).read()
        except IOError:
            self.image_has_error = True
            self.image_error_code = 'FETCH ERROR'
            logging.debug('   ---> [%s] ~SN~FRFetch Error on image: %s' % (
                            story.story_guid, self.image_guid))
            raise IOError('Fetch Error')

        # Now we have the buffer, test if it's an image
        num_trial = 0
        while num_trial < 3:
            num_trial +=1
            try:
                image = Image.open(cStringIO.StringIO(self.image_buffer))
                image.load()
                break
            except (IOError,IndexError),e:
                # file download corrupted, retry
                if 'truncated' in str(e) or 'index' in str(e):
                    self.image_buffer = self._fetch_image(story_url=story.story_guid).read()
                    logging.debug('  ---> [%s] ~FYRetry with image: %s' % (
                        story.story_guid, self.image_guid))
                    continue
                self.image_has_error = True
                self.image_error_code = 'DECODE ERROR'
                if verbose:
                    logging.debug('   ---> [%s] ~SN~FRDecode Error on image: %s' % (
                                story.story_guid, self.image_guid))
                raise IOError('Decode Error')

        self.image_type = image.format
        self.image_size = len(self.image_buffer)
        self.image_width, self.image_height = image.size
        # discard images with small width or height
        if self.image_width < 150 or self.image_height < 150:
            # I think to raise an Exception is better..
            # since there are many images with only 1 pixel and different url, see CNN
            # should add some regrex patterns to filter the indifferent images to save bandwidth
            if verbose:
                logging.debug('   ---> [%s] ~SN~FRLow definition~FW on image:~FR %s' % (
                            story.story_guid, self.image_guid))
            raise AssertionError('Low definition')
            self.image_has_error = True
            self.image_error_code = 'LOW DEFINITION'
            return

        # now upload the image to FastDFS
        ret = self._upload_by_buffer(client=client) # much better if a client is given
        if ret['Status'] == 'Upload successed.':
            self.image_remote_id = ret['Remote file_id']
        else:
            self.image_has_error = True
            self.image_error_code = 'UPLOAD ERROR'
            logging.debug('   ---> [%s] ~SN~FRUpload Error on image: %s' % (
                            story.story_guid, self.image_guid))
            return
        if verbose:
            logging.debug(' -Successfully create image: %s' % self.image_guid)

    @property
    def guid_hash(self):
        return hashlib.sha1(self.image_guid).hexdigest()[:8]

    @classmethod
    def get_by_url(cls,url):
        url_hash = hashlib.sha1(url).hexdigest()[:8]
        images = cls.objects(image_hash=url_hash)
        if len(images) == 0:
            return None
        for image in images:
            if image.image_guid == url:
                return image
        return None

    @classmethod
    def exists_image(cls,url):
        if cls.get_by_url(url) == None:
            return False
        else:
            return True


    def _fetch_image(self, story_url=None, cow=False):
        handlers = []
        if cow:
            handlers.append(urllib2.ProxyHandler({'http':settings.COW_PROXY_HANDLER,\
                'https':settings.COW_PROXY_HANDLER}))
        opener = urllib2.build_opener(*tuple(handlers))
        request = urllib2.Request(self.image_guid)
        request.add_header('User-Agent','Mozilla/5.0')
        # some sites may limit image access
        if story_url:
            request.add_header('Referer',story_url)
        opener.addheaders = [] # RMK - must clear so we only send our custom User-Agent

        try:
            return opener.open(request)
        finally:
            opener.close() # JohnD

    def _upload_by_buffer(self,client=None):
        if not client:
            client = Fdfs_client(settings.FDFS_CLIENT_CONF_PATH)
        file_ext_name = self.image_type.lower()
        meta_dict = {
            'ext_name' : file_ext_name,
            'file_size' : self.image_size,
            'width' : self.image_width,
            'height' : self.image_height
        }
        return client.upload_by_buffer(self.image_buffer, file_ext_name=file_ext_name, meta_dict=meta_dict)

    def save_append_story(self,story):
        story_id = str(story.id)
        if story_id not in self.story_ids:
            self.story_ids.append(story_id)
            self.save()

    def save(self,*args,**kwargs):
        try:
            self.last_reference_date = datetime.datetime.utcnow()
            super(MImage,self).save(*args,**kwargs)
        except Exception,e:
            logging.error('Saving MImage with: ~FR%s~FW',str(e))

    def delete(self,*args,**kwargs):
        client = Fdfs_client(settings.FDFS_CLIENT_CONF_PATH)
        if self.image_remote_id:
            try:
                client.delete_file(str(self.image_remote_id))
            except DataError:
                logging.debug('File Not Exists:',self.image_remote_id)
                pass
        # deal with reference id?
        super(MImage, self).delete(*args, **kwargs)

    @classmethod
    def drop(cls,*args,**kwargs):
        images = cls.objects.all()
        for image in images:
            image.delete()


