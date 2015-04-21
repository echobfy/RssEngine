import datetime
import os
import shutil
import time
import redis
from celery.task import Task
from utils import log as logging
from utils.net_monitor import NetworkMonitor
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
            #settings.LOG_TO_STREAM = True
            now = datetime.datetime.utcnow()
            start = time.time()
            r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)

            # get the size of tasked_feeds
            tasked_feeds_size = r.zcard('tasked_feeds')

            hour_ago = now - datetime.timedelta(hours=1)
            r.zremrangebyscore('fetched_feeds_last_hour', 0, int(hour_ago.strftime('%s')))

            # get the feed_ids in the scheduled_updates, and delete them.
            now_timestamp = int(now.strftime("%s"))
            queued_feeds = r.zrangebyscore('scheduled_updates', 0, now_timestamp)
            r.zremrangebyscore('scheduled_updates', 0, now_timestamp)

            logging.debug(" ---> ~SN~FBQueuing ~SB%s~SN stale feeds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            len(queued_feeds),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))

            # and add to queued_feeds the feed_ids which come from scheduled_updates.
            if len(queued_feeds) > 0:
                r.sadd('queued_feeds', *queued_feeds)

            logging.debug(" ---> ~SN~FBQueuing ~SB%s~SN stale feeds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            len(queued_feeds),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))

            # if the number of fetch task is less than 5000, and then get no more than 5000 feed_ids from queued_feeds
            if tasked_feeds_size < 5000:
                feeds = r.srandmember('queued_feeds',5000)
                # this method will delete the feed_ids in queued_feeds, and add them to tasked_feeds, and package feed_id
                # to a fetch task and distribute.
                # Note: feed_id delete from tasked_feeds only after the fetch is over. 
                # the method is sync, but task.apply_async is not.
                Feed.task_feeds(feeds, verbose=True)
                active_count = len(feeds)
            else:
                logging.debug(" ---> ~SN~FBToo many tasked feeds. ~SB%s~SN tasked." % tasked_feeds_size)
                active_count = 0
            cp1 = time.time()

            # order_by('?') is to order randomly
            # If the system is started, and the scheduled_updates(Sorted_Set) is null, 
            # this method will force the feed that is not fetched once to schedule.
            # And this method will also force the new feed to refresh.
            refresh_feeds = Feed.objects.filter(
                fetched_once=False,
                active_subscribers__gte=1
            ).order_by('?')[:100]
            refresh_count = refresh_feeds.count()
            cp2 = time.time()

            # Mistakenly inactive feeds.
            # If the feed is not fetched in 10 minutes, we assume that the feeds are maybe wrong or fetch error.
            hours_ago = (now - datetime.timedelta(minutes=10)).strftime('%s')
            old_tasked_feeds = r.zrangebyscore('tasked_feeds', 0, hours_ago)
            inactive_count = len(old_tasked_feeds)
            if inactive_count:
                r.zremrangebyscore('tasked_feeds', 0, hours_ago)
                for feed_id in old_tasked_feeds:
                    # add this feed_id in error_feeds, and set next scheduled update for it.
                    r.zincrby('error_feeds', feed_id, 1)
                    feed = Feed.get_by_id(feed_id)
                    feed.set_next_scheduled_update()
                logging.debug(" ---> ~SN~FBRe-queuing ~SB%s~SN dropped feeds (~SB%s/%s~SN queued/tasked)" % (
                                inactive_count,
                                r.scard('queued_feeds'),
                                r.zcard('tasked_feeds')))
            cp3 = time.time()

            # If the system is halt or stop, the time of next_scheduled_update will earlier than now.
            # and the feeds whose next_scheduled_update is earlier than now is supposed to schedule now.
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
            r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)

            options = {
            #    'quick': float(MStatistics.get('quick_fetch', 0)),
            #    'compute_scores': compute_scores,
            #    'mongodb_replication_lag': mongodb_replication_lag,
            }

            if not isinstance(feed_pks, list):
                feed_pks = [feed_pks]

            for feed_pk in feed_pks:
                feed = Feed.get_by_id(feed_pk)
                # if feed is null or feed.pk != feed_pk, then delete the feed_pk from tasked_feeds
                # otherwise delete the feed_pk after the fetch is over.
                if not feed or feed.pk != int(feed_pk):
                    logging.info(" ---> ~FRRemoving feed_id %s from tasked_feeds queue, points to %s..." % 
                                        (feed_pk, feed and feed.pk))
                    r.zrem('tasked_feeds', feed_pk)
                if feed:
                    feed.update(**options)
        except Exception, e:
            logging.error(str(e) + traceback.format_exc() + '\n' + 'error from:  UpdateFeeds\n')
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
        

class FreezeFeeds(Task):
    name = 'freeze-feeds'
    max_retries = 0
    ignore_result = False

    def run(self, feed_pk, **kwargs):
        from apps.rss_feeds.models import MStory
        MStory.freeze_feed(feed_pk)
        logging.info('---> ~FYProcess freeze feed %d done!~FW'%feed_pk)


