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

# tasked_feeds（有序集合）：存储的是已经将任务分发出去，但是还未执行完成的feed_id
# fetched_feeds_last_hour（有序集合）：存储的是相对于现在过去一小时已经抓过的feed_id，与现在相比一小时之内的还在该集合内
# scheduled_updates（有序集合）：存储的是将要调度来抓取的feed_id
# queued_feeds（集合）：存储是正常排队的feed_id，将要做成task分发出去


class TaskFeeds(Task):
    name = 'task-feeds'

    def run(self, **kwargs):
        try:
            from apps.rss_feeds.models import Feed
            settings.LOG_TO_STREAM = True
            now = datetime.datetime.utcnow()
            start = time.time()
            r = redis.Redis(connection_pool=settings.REDIS_FEED_POOL)

            # 得到tasked_feeds（有序集合）中的个数
            tasked_feeds_size = r.zcard('tasked_feeds')

            hour_ago = now - datetime.timedelta(hours=1)
            # 移除一小时之前抓过的feed_id，一小时之内抓过的还在此集合内
            r.zremrangebyscore('fetched_feeds_last_hour', 0, int(hour_ago.strftime('%s')))

            # 获得scheduled_updates中到目前为止所以的feed_id，并删除
            now_timestamp = int(now.strftime("%s"))
            queued_feeds = r.zrangebyscore('scheduled_updates', 0, now_timestamp)
            r.zremrangebyscore('scheduled_updates', 0, now_timestamp)

            logging.debug(" ---> ~SN~FBQueuing ~SB%s~SN stale feeds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            len(queued_feeds),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))

            # 从可以调度的队列中获取全部的feed_id，扔到排队队列中
            if len(queued_feeds)>0:
                r.sadd('queued_feeds', *queued_feeds)

            logging.debug(" ---> ~SN~FBQueuing ~SB%s~SN stale feeds (~SB%s~SN/~FG%s~FB~SN/%s tasked/queued/scheduled)" % (
                            len(queued_feeds),
                            r.zcard('tasked_feeds'),
                            r.scard('queued_feeds'),
                            r.zcard('scheduled_updates')))

            # 如果已经分发出去的任务个数小于5000，就从排队的queued_feeds中获取至多5000的feed_id，拿去
            if tasked_feeds_size < 5000:
                feeds = r.srandmember('queued_feeds',5000)
                # 该函数会将feeds中的id从queued_feeds中删除，然后添加到tasked_feeds中，再制作成任务分发出去
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
                # 如果该feed_id的feed被删除，或者该feed的pk被修改
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