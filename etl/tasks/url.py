# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

from os import path
import json
import luigi
from luigi import LocalTarget
import scrapelib
from bs4 import BeautifulSoup

from etl import utils
from etl.settings import get_config

scraper = scrapelib.Scraper(requests_per_minute=30, retry_attempts=3)


class URLFetchTask(luigi.Task):

    url = luigi.Parameter(default=None)

    def run(self):
        result = scraper.get(self.url)
        with self.output().open("w") as fout:
            fout.write(result.content)

    def output(self):
        return LocalTarget(path.dirname(path.abspath(__file__)) + "/../../tmp/url_fetch/%s/%s.html" % (utils.master_folder_date_format(utils.jst_now()), utils.hash_digest([self.url])))


class URLParseTask(luigi.Task):

    url = luigi.Parameter(default=None)

    def requires(self):
        return URLFetchTask(url=self.url)

    def run(self):
        with self.input().open('r') as input:
            soup = BeautifulSoup(input, "html5lib")
            meta = {'title': soup.title.string.encode(
                'utf-8'), 'description': '', 'keywords': ''}
            description = soup.findAll(
                attrs={"name": "Description", "url": self.url})
            if not description:
                description = soup.findAll(attrs={"name": "description"})
            if description:
                meta['description'] = description[
                    0].attrs['content'].encode('utf-8')

            keywords = soup.findAll(attrs={"name": "Keywords"})
            if not description:
                description = soup.findAll(attrs={"name": "keywords"})
            if description:
                meta['keywords'] = description[
                    0].attrs['content'].encode('utf-8')

            with self.output().open("w") as fout:
                fout.write(json.dumps(meta))

    def output(self):
        return LocalTarget(path.dirname(path.abspath(__file__)) + "/../../tmp/url_parse/%s/%s.json" % (utils.master_folder_date_format(utils.jst_now()), utils.hash_digest([self.url])))
