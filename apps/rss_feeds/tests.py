from django.test import TestCase

from models import Feed

class FeedTests(TestCase):
	def test_unicode(self):
		pass

class TextImporterTest(TestCase):
	def setUp(self):
		Feed.objects.create()

	def testTextImporter(self):
		pass