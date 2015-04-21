import requests
import zlib
from django.conf import settings
from socket import error as SocketError
from mongoengine.queryset import NotUniqueError
from vendor.readability import readability
from utils.feed_functions import timelimit, TimeoutError
import urllib2
from utils import log as logging
import httplib
from BeautifulSoup import BeautifulSoup
import traceback
from django.core.mail import mail_admins
import gzip, cStringIO


class TextImporter:

    '''
    According to the story_permalink, the urllib2 go to fetch 
    the html document, and parse the content of the document
    and store it.

    '''

    def __init__(self, story, request=None):
        self.story = story
        self.request = request

    @property
    def headers(self):
        return {
            'User-Agent': 'NewsBlur Content Fetcher - 100 subscribers '
                          '(Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_1) '
                          'AppleWebKit/534.48.3 (KHTML, like Gecko) Version/5.1 '
                          'Safari/534.48.3)'
            }

    def fetch(self, skip_save=False):
        try:
            resp = self.fetch_request();
        except TimeoutError:
            logging.user(self.request, "~SN~FRFailed~FY to fetch ~FGoriginal text~FY: timed out")
            resp = None
        except requests.exceptions.TooManyRedirects:
            logging.user(self.request, "~SN~FRFailed~FY to fetch ~FGoriginal text~FY: too many redirects")
            resp = None 
        if not resp: return

        try:
            text = resp.text
        except (LookupError, TypeError):
            text = resp.content
        
        charset_declared = 'charset' in resp.headers.get('content-type', "")
        if resp.encoding and resp.encoding != 'utf-8' and not charset_declared:
            try:
                text = text.encode(resp.encoding)
            except (LookupError, UnicodeEncodeError):
                pass
        original_text_doc = readability.Document(text, url=resp.url)
        try:
            content = original_text_doc.summary(html_partial=True)
        except readability.Unparseable:
            return

        # try:
        #     title = original_text_doc.title()
        # except TypeError:
        #     title = ""
        # url = resp.url
        
        if content:
            if self.story and not skip_save:
                self.story.original_text_z = zlib.compress(content)
                try:
                    self.story.save()
                except NotUniqueError:
                    pass
            logging.user(self.request, ("~SN~FYFetched ~FGoriginal text~FY: now ~SB%s bytes~SN vs. was ~SB%s bytes" % (
                len(unicode(content)),
                self.story and self.story.story_content_z and len(zlib.decompress(self.story.story_content_z))
            )), warn_color=False)
        else:
            logging.user(self.request, ("~SN~FRFailed~FY to fetch ~FGoriginal text~FY: was ~SB%s bytes" % (
                self.story and self.story.story_content_z and len(zlib.decompress(self.story.story_content_z))
            )), warn_color=False)
        

        return content


    @timelimit(60)
    def fetch_request(self):
        try:
            #proxies = {'http':settings.COW_PROXY_HANDLER, https':settings.COW_PROXY_HANDLER}
            #r = requests.get(self.story.story_permalink, headers=self.headers, verify=False, proxies=proxies)
            r = requests.get(self.story.story_permalink, headers=self.headers, verify=False)
            r.connection.close()
        except (AttributeError, SocketError, requests.ConnectionError,
                requests.models.MissingSchema, requests.sessions.InvalidSchema), e:
            logging.user(self.request, "~SN~FRFailed~FY to fetch ~FGoriginal text~FY: %s" % e)
            return
        return r
