import sys
import os
import logging
import datetime

import redis
from mongoengine import connect

SEND_ERROR_MAILS = False

# ===========================
# = Directory Declaractions =
# ===========================
CURRENT_DIR   = os.path.dirname(__file__)
LOG_FILE      = os.path.join(CURRENT_DIR, 'logs/django_rss.log')
UTILS_ROOT    = os.path.join(CURRENT_DIR, 'utils')
VENDOR_ROOT   = os.path.join(CURRENT_DIR, 'vendor')

# ==============
# = PYTHONPATH =
# ==============
if '/utils' not in ' '.join(sys.path):
    sys.path.append(UTILS_ROOT)

if '/vendor' not in ' '.join(sys.path):
    sys.path.append(VENDOR_ROOT)


DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    ('Bai fuyu','379348913@qq.com'),
)

# =================
# = For Rss Feeds =
# =================
DEFAULT_CHARSET = 'UTF-8'
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'rssFeed',
        'USER': 'udms',
        'PASSWORD': '123456',
        'HOST': '10.13.91.251',
        'PORT': '',        
    }
}

# ==========
# = Celery =
# ==========

import djcelery
djcelery.setup_loader()
CELERY_ROUTES = {
    "work-queue": {
        "queue": "work_queue",
        "binding_key": "work_queue"
    },
    "update-feeds": {
        "queue": "update_feeds",
        "binding_key": "update_feeds"
    },
    "beat-tasks": {
        "queue": "beat_tasks",
        "binding_key": "beat_tasks"
    },
    "net-monitor-task": {
        "queue": "beat_tasks",
        "binding_key": "net_monitor_task"
    },
}
CELERY_QUEUES = {
    "work_queue": {
        "exchange": "work_queue",
        "exchange_type": "direct",
        "binding_key": "work_queue",
    },
    "update_feeds": {
        "exchange": "update_feeds",
        "exchange_type": "direct",
        "binding_key": "update_feeds"
    },
    "beat_tasks": {
        "exchange": "beat_tasks",
        "exchange_type": "direct",
        "binding_key": "beat_tasks"
    },
    "beat_feeds_task": {
        "exchange": "beat_feeds_task",
        "exchange_type": "direct",
        "binding_key": "beat_feeds_task"
    },
}
CELERY_DEFAULT_QUEUE = "work_queue"

CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_IMPORTS              = ("apps.rss_feeds.tasks",)
                               # "apps.statistics.tasks",)
CELERYD_CONCURRENCY         = 3
CELERY_IGNORE_RESULT        = True
CELERY_ACKS_LATE            = True # Retry if task fails
CELERYD_MAX_TASKS_PER_CHILD = 10
CELERYD_TASK_TIME_LIMIT     = 12 * 30
CELERY_DISABLE_RATE_LIMITS  = True
SECONDS_TO_DELAY_CELERY_EMAILS = 60

CELERYBEAT_SCHEDULE = {
    'task-feeds': {
        'task': 'task-feeds',
        'schedule': datetime.timedelta(minutes=1),
        'options': {'queue': 'beat_feeds_task'},
    },
}


# =========
# = Redis =
# =========

REDIS = {
    'host': 'localhost',
}
REDIS_STORY = {
    'host': 'localhost',
}
CELERY_REDIS_DB = 4
SESSION_REDIS_DB = 5



BROKER_BACKEND = "redis"
BROKER_URL = "redis://%s:6379/%s" % (REDIS['host'], CELERY_REDIS_DB)
CELERY_RESULT_BACKEND = BROKER_URL

CACHES = {
    'default': {
        'BACKEND': 'redis_cache.RedisCache',
        'LOCATION': '%s:6379' % REDIS['host'],
        'OPTIONS': {
            'DB': 6,
            'PARSER_CLASS': 'redis.connection.HiredisParser'
        },
    },
}

# =========
# = Redis =
# =========
REDIS_POOL               = redis.ConnectionPool(host=REDIS['host'], port=6379, db=0)
REDIS_ANALYTICS_POOL     = redis.ConnectionPool(host=REDIS['host'], port=6379, db=2)
REDIS_STATISTICS_POOL    = redis.ConnectionPool(host=REDIS['host'], port=6379, db=3)
REDIS_FEED_POOL          = redis.ConnectionPool(host=REDIS['host'], port=6379, db=4)
REDIS_NETWORK_POOL       = redis.ConnectionPool(host=REDIS['host'], port=6379, db=6)
REDIS_STORY_HASH_POOL    = redis.ConnectionPool(host=REDIS_STORY['host'], port=6379, db=1)

# ===================
# = Network Monitor =
# ===================
REDIS_NETWORK_LOG_MAX = 1500
REDIS_NETWORK_LOG_NAME= "network_log"

# =========
# = Mongo =
# =========
MONGO_DB = {
	'host': '10.13.91.251:27017',
    'name': 'RssEngine',
    'username': 'udms',
    'password': '123456',
}

MONGO_DB_DEFAULTS = {
    'name': 'RssEngine',
	'host': '10.13.91.251:27017',
    'alias': 'default',
}
MONGO_DB = dict(MONGO_DB_DEFAULTS, **MONGO_DB)
MONGODB = connect(MONGO_DB.pop('name'), **MONGO_DB)

# =================
# = Elasticsearch =
# =================
ELASTICSEARCH_HOSTS = ['localhost:9200']


# Hosts/domain names that are valid for this site; required if DEBUG is False
# See https://docs.djangoproject.com/en/1.5/ref/settings/#allowed-hosts
ALLOWED_HOSTS = []

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'Asia/Shanghai'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = False

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
# USE_L10N = False

# If you set this to False, Django will not use timezone-aware datetimes.
# USE_TZ = False

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/var/www/example.com/media/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = '/media/'

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT   = ''

# URL prefix for static files.
# Example: "http://example.com/static/", "http://static.example.com/"
STATIC_URL = '/static/'
# Additional locations of static files
STATICFILES_DIRS = (
    os.path.join(CURRENT_DIR, 'static'),
)

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
#    'django.contrib.staticfiles.finders.DefaultStorageFinder',
)

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'lb-o1_oa4pkb2z03j)p&&i0(e3a-_ys297j#1#q23k3blz80$4'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
#     'django.template.loaders.eggs.Loader',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
)

ROOT_URLCONF = 'urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'wsgi.application'

TEMPLATE_DIRS = (
    os.path.join(CURRENT_DIR, 'templates'),
)

INSTALLED_APPS = (
    'django_extensions',
    #'django.contrib.auth',
    #'django.contrib.contenttypes',
    #'django.contrib.sessions',
    #'django.contrib.sites',
    #'django.contrib.messages',
    #'django.contrib.admin',
    # 'django.contrib.admindocs',
    'django.contrib.staticfiles',
    'djcelery',
    'apps.rss_feeds',
    'apps.search',
    'apps.statistics',    
    'utils',
    'vendor',
)

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOG_LEVEL = logging.DEBUG
LOG_TO_STREAM = False

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '[%(asctime)-12s] %(message)s',
            'datefmt': '%b %d %H:%M:%S'
        },
        'simple': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'django.utils.log.NullHandler',
        },
        'console':{
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
        'log_file':{
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_FILE,
            'maxBytes': 16777216, # 16megabytes
            'formatter': 'verbose'
        },
        # 'mail_admins': {
        #     'level': 'ERROR',
        #     'class': 'django.utils.log.AdminEmailHandler',
        #     'filters': ['require_debug_false'],
        #     'include_html': True,
        # }
        # Modified By Xinyan Lu: no mail notice
        'log_error': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': LOG_FILE+'.ERROR.log',
            'maxBytes': 16777216, # 16megabytes
            'formatter': 'verbose',
            # without this line, there would be a warning. Xinyan Lu
            #'filters': ['require_debug_false'],
        },
    },
    'loggers': {
        'django.request': {
            'handlers': ['console', 'log_file'],
            'level': 'ERROR',
            'propagate': True,
        },
        'django.db.backends': {
            'handlers': ['null'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'newsblur': {
            'handlers': ['console', 'log_file','log_error'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'apps': {
            'handlers': ['log_file'],
            'level': 'INFO',
            'propagate': True,
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
}

#============================
#= COW Corss the Green Wall =
#============================
COW_PROXY_HANDLER = 'http://localhost:7777'

#=======================
#= Image URL Blacklist =
#=======================
IMAGE_URL_BLACKLIST = os.path.join(CURRENT_DIR,'conf/image_url_blacklist')

#=====================
#= Mail Notification =
#=====================
MAIL_HOST = "smtp.126.com"
MAIL_USER = "rss_feeds"
MAIL_PASS = "zjurss"
MAIL_POSTFIX = "126.com"
MAIL_NOTIFY_LIST = ['515822895@qq.com']
