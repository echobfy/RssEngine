import datetime
import os
import shutil
import time
import redis
from celery.task import Task
from utils import log as logging
from utils.net_monitor import NetworkMonitor
#from utils import s3_utils as s3 #---commented by songjun
from django.conf import settings
import sys
import traceback
from django.core.mail import mail_admins

class NetMonitorTask(Task):
    name = 'net-monitor-task'

    def run(self, **kwargs):
        logging.debug("%s [MonitorTask] start" % (time.strftime("%Y-%m-%d %H:%M:%S")))
        NetworkMonitor.test()
        logging.debug("%s [MonitorTask] finish" % (time.strftime("%Y-%m-%d %H:%M:%S")))


class TaskFeeds(Task):
    name = 'task-feeds'

    def run(self, **kwargs):
        try:
            from apps.rss_feeds.models import Feed
            settings.LOG_TO_STREAM = True
            now = datetime.datetime.utcnow()
            start = time.time()
            r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)
            tasked_feeds_size = r.zcard('tasked_feeds')

            hour_ago = now - datetime.timedelta(hours=1)
            r.zremrangebyscore('fetched_feeds_last_hour', 0, int(hour_ago.strftime('%s')))

            now_timestamp = int(now.strftime("%s"))
            queued_feeds = r.zrangebyscore('scheduled_updates', 0, now_timestamp)
            print len(queued_feeds)
            r.zremrangebyscore('scheduled_updates', 0, now_timestamp)
            logging.debug(" ---> ~SN~FBQueuing ~SB%s~SN stale feeds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            len(queued_feeds),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))
            print len(queued_feeds)
            if len(queued_feeds)>0:
                r.sadd('queued_feeds', *queued_feeds)
            logging.debug(" ---> ~SN~FBQueuing ~SB%s~SN stale feeds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            len(queued_feeds),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))

            # Regular feeds
            if tasked_feeds_size < 5000:
                feeds = r.srandmember('queued_feeds',5000)
                Feed.task_feeds(feeds, verbose=True)
                active_count = len(feeds)
            else:
                logging.debug(" ---> ~SN~FBToo many tasked feeds. ~SB%s~SN tasked." % tasked_feeds_size)
                active_count = 0
            cp1 = time.time()

            # Force refresh feeds
            refresh_feeds = Feed.objects.filter(
                active=True,
                fetched_once=False,
                active_subscribers__gte=1
            ).order_by('?')[:100]
            refresh_count = refresh_feeds.count()
            cp2 = time.time()

            # Mistakenly inactive feeds
            hours_ago = (now - datetime.timedelta(minutes=10)).strftime('%s')
            old_tasked_feeds = r.zrangebyscore('tasked_feeds', 0, hours_ago)
            inactive_count = len(old_tasked_feeds)
            if inactive_count:
                r.zremrangebyscore('tasked_feeds', 0, hours_ago)
                # r.sadd('queued_feeds', *old_tasked_feeds)
                for feed_id in old_tasked_feeds:
                    r.zincrby('error_feeds', feed_id, 1)
                    feed = Feed.get_by_id(feed_id)
                    feed.set_next_scheduled_update()
                logging.debug(" ---> ~SN~FBRe-queuing ~SB%s~SN dropped feeds (~SB%s/%s~SN queued/tasked)" % (
                                inactive_count,
                                r.scard('queued_feeds'),
                                r.zcard('tasked_feeds')))
            cp3 = time.time()

            old = now - datetime.timedelta(days=1)
            old_feeds = Feed.objects.filter(
                next_scheduled_update__lte=old,
                active_subscribers__gte=1
            ).order_by('?')[:500]
            old_count = old_feeds.count()
            cp4 = time.time()

            logging.debug(" ---> ~FBTasking ~SB~FC%s~SN~FB/~FC%s~FB (~FC%s~FB/~FC%s~SN~FB) feeds... (%.4s/%.4s/%.4s/%.4s)" % (
                active_count,
                refresh_count,
                inactive_count,
                old_count,
                cp1 - start,
                cp2 - cp1,
                cp3 - cp2,
                cp4 - cp3
            ))

            Feed.task_feeds(refresh_feeds, verbose=True)
            Feed.task_feeds(old_feeds, verbose=True)

            logging.debug(" ---> ~SN~FBTasking took ~SB%s~SN seconds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            int((time.time() - start)),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))
        except Exception, e:
            import traceback
            traceback.print_exc()
            logging.error(str(e))
            if settings.SEND_ERROR_MAILS:
                mail_admins("Error in Task-Feeds",str(e)+'\n'+traceback.format_exc())

class UpdateFeeds(Task):
    name = 'update-feeds'
    max_retries = 0
    ignore_result = False

    def run(self, feed_pks, **kwargs):
        try:
            from apps.rss_feeds.models import Feed
            #from apps.statistics.models import MStatistics
            r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)
            #mongodb_replication_lag = int(MStatistics.get('mongodb_replication_lag', 0))
            #compute_scores = bool(mongodb_replication_lag < 10)

            options = {
            #    'quick': float(MStatistics.get('quick_fetch', 0)),
            #    'compute_scores': compute_scores,
            #    'mongodb_replication_lag': mongodb_replication_lag,
            }

            if not isinstance(feed_pks, list):
                feed_pks = [feed_pks]

            for feed_pk in feed_pks:
                feed = Feed.get_by_id(feed_pk)
                if not feed or feed.pk != int(feed_pk):
                    logging.info(" ---> ~FRRemoving feed_id %s from tasked_feeds queue, points to %s..." % (feed_pk, feed and feed.pk))
                    r.zrem('tasked_feeds', feed_pk)
                if feed:
                    feed.update(**options)
        except Exception, e:
            logging.error(str(e)+\
                traceback.format_exc()+'\n'+\
                'error from:  UpdateFeeds\n')
            if settings.SEND_ERROR_MAILS:
                mail_admins("Error in UpdateFeeds",str(e)+'\n'+traceback.format_exc())


class UpdateFeedImages(Task):
    name = 'update-feed-images'
    max_retries = 0
    ignore_result = False

    def run(self,feed_pk,**kwargs):
        from apps.rss_feeds.models import MStory

        stories = MStory.objects(story_feed_id=feed_pk)
        if len(stories): #is sort by story-data
            for story in stories:
                # start = time.time()
                story.fetch_reference_images()
                # num_valid_urls = 0
                # for image_id in story.image_ids:
                #     if len(image_id) > 20:
                #         num_valid_urls +=1

                # delta = time.time() - start
                # logging.info('Process ~FY%d~FW[~FB%d~FW] urls in ~FG%.4s~FW seconds.' % (
                #     num_valid_urls,len(story.image_urls),delta))
        logging.info('---> ~FYProcess feed %d done!~FW'%feed_pk)

class NewFeeds(Task):
    name = 'new-feeds'
    max_retries = 0
    ignore_result = True

    def run(self, feed_pks, **kwargs):
        from apps.rss_feeds.models import Feed
        if not isinstance(feed_pks, list):
            feed_pks = [feed_pks]

        options = {}
        for feed_pk in feed_pks:
            feed = Feed.get_by_id(feed_pk)
            if not feed: continue
            feed.update(options=options)

class PushFeeds(Task):
    name = 'push-feeds'
    max_retries = 0
    ignore_result = True

    def run(self, feed_id, xml, **kwargs):
        from apps.rss_feeds.models import Feed
        from apps.statistics.models import MStatistics

        mongodb_replication_lag = int(MStatistics.get('mongodb_replication_lag', 0))
        compute_scores = bool(mongodb_replication_lag < 60)

        options = {
            'feed_xml': xml,
            'compute_scores': compute_scores,
            'mongodb_replication_lag': mongodb_replication_lag,
        }
        feed = Feed.get_by_id(feed_id)
        if feed:
            feed.update(options=options)
# class BackupMongo commented by songjun
'''
class BackupMongo(Task):
    name = 'backup-mongo'
    max_retries = 0
    ignore_result = True

    def run(self, **kwargs):
        COLLECTIONS = "classifier_tag classifier_author classifier_feed classifier_title userstories starred_stories shared_stories category category_site sent_emails social_profile social_subscription social_services statistics feedback"

        date = time.strftime('%Y-%m-%d-%H-%M')
        collections = COLLECTIONS.split(' ')
        db_name = 'newsblur'
        dir_name = 'backup_mongo_%s' % date
        filename = '%s.tgz' % dir_name

        os.mkdir(dir_name)

        for collection in collections:
            cmd = 'mongodump  --db %s --collection %s -o %s' % (db_name, collection, dir_name)
            logging.debug(' ---> ~FMDumping ~SB%s~SN: %s' % (collection, cmd))
            os.system(cmd)

        cmd = 'tar -jcf %s %s' % (filename, dir_name)
        os.system(cmd)

        logging.debug(' ---> ~FRUploading ~SB~FM%s~SN~FR to S3...' % filename)
        s3.save_file_in_s3(filename)
        shutil.rmtree(dir_name)
        os.remove(filename)
        logging.debug(' ---> ~FRFinished uploading ~SB~FM%s~SN~FR to S3.' % filename)
'''

class ScheduleImmediateFetches(Task):

    def run(self, feed_ids, **kwargs):
        from apps.rss_feeds.models import Feed

        if not isinstance(feed_ids, list):
            feed_ids = [feed_ids]

        Feed.schedule_feed_fetches_immediately(feed_ids)


class SchedulePremiumSetup(Task):

    def run(self, feed_ids, **kwargs):
        from apps.rss_feeds.models import Feed

        if not isinstance(feed_ids, list):
            feed_ids = [feed_ids]

        Feed.setup_feeds_for_premium_subscribers(feed_ids)

class FreezeFeeds(Task):
    name = 'freeze-feeds'
    max_retries = 0
    ignore_result = False

    def run(self,feed_pk,**kwargs):
        from apps.rss_feeds.models import MStory
        MStory.freeze_feed(feed_pk)
        logging.info('---> ~FYProcess freeze feed %d done!~FW'%feed_pk)