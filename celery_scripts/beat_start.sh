#!/bin/bash
cd ..
python manage.py celery worker -B -Q beat_feeds_task,beat_tasks --loglevel=info --autoreload
echo start rss_beat OK!
