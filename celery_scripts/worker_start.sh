#!/bin/bash
cd ..
python manage.py celery worker -c 50 -Q update_feeds --loglevel=info
echo restart rss_worker OK!
