# -*- coding: utf-8 -*-
import pyes
from pyes.query import Search,QueryStringQuery,FilteredQuery, FuzzyQuery, TextQuery, PrefixQuery
from pyes.filters import RangeFilter
from pyes.highlight import HighLighter
from pyes.utils import ESRange
from django.conf import settings
from utils import log as logging

# Add by Xinyan Lu : To support story search
class SearchStory:
    
    ES = pyes.ES(settings.ELASTICSEARCH_HOSTS)
    name = "stories"
    
    @classmethod
    def create_elasticsearch_mapping(cls):
        # 新建SearchStory索引库，相当于create database操作
        cls.ES.create_index("%s-index" % cls.name)
        mapping = {
            'title': {
                'boost': 2.0,
                'index': 'analyzed',    #使用分词器
                'store': 'yes',
                'type': 'string',
                "term_vector" : "with_positions_offsets"
            },
            'content': {
                'boost': 1.0,
                'index': 'analyzed',
                'store': 'yes',
                'type': 'string',
                "term_vector" : "with_positions_offsets"
            },
            'author': {
                'boost': 1.0,
                'index': 'analyzed',
                'store': 'yes',
                'type': 'string',   
            },
            'db_id': {
                'index': 'not_analyzed',
                'store': 'yes',
                'type': 'string',   
            },
            'feed_id': {
                'store': 'yes',
                'type': 'integer'
            },
            'date': {
                'store': 'yes',
                'type': 'date',
            },
            'frozen':{
                'store': 'yes',
                'type': 'boolean',
            },
        }
        cls.ES.put_mapping("%s-type" % cls.name, {'properties': mapping}, ["%s-index" % cls.name])
        
    @classmethod
    def index(cls, story_id, story_title, story_content, story_author, story_date, db_id, frozen=False):
        doc = {
            "content": story_content,
            "title": story_title,
            "author": story_author,
            "date": story_date,
            "db_id": db_id,
            "frozen": frozen,
        }
        cls.ES.index(doc, "%s-index" % cls.name, "%s-type" % cls.name, story_id)
        
    @classmethod
    def query(cls, text):
        text = text.strip()
        cls.ES.refresh()
        q = StringQuery(text)
        highlighter = HighLighter(['<em>'],['</em>'])
        # highlighter = HighLighter()
        s = Search(q, highlight=highlighter)
        s.add_highlight('title')
        s.add_highlight('content')
        results = cls.ES.search(s, indices=['%s-index' % cls.name])
        # logging.user(user, "~FGSearch ~FCsaved stories~FG for: ~SB%s" % text)
        
        if not results.total:
            # logging.user(user, "~FGSearch ~FCsaved stories~FG by title: ~SB%s" % text)
            q = FuzzyQuery('title', text)
            results = cls.ES.search(q)
            
        if not results.total:
            # logging.user(user, "~FGSearch ~FCsaved stories~FG by content: ~SB%s" % text)
            q = FuzzyQuery('content', text)
            results = cls.ES.search(q)
            
        if not results.total:
            # logging.user(user, "~FGSearch ~FCsaved stories~FG by author: ~SB%s" % text)
            q = FuzzyQuery('author', text)
            results = cls.ES.search(q)
            
        return results
