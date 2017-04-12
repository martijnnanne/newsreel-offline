
from evaluation_stats import Stats
import re
from abc import ABCMeta
import abc

import gzip

class GenericRecommender(metaclass=ABCMeta):
    def __init__(self, BASEDIR):
        self.BASEDIR = BASEDIR
        self.evaluation = Stats()

    def init_new_day(self):
        self.evaluation = Stats()

    def true_rec(self, nextevent):
        true_rec_json = nextevent[self.recs_idx]
        return re.search(r"\[([A-Za-z0-9_]+)\]", true_rec_json).group(1)


    @abc.abstractmethod
    def get_recommendation(self, nextevent):
        raise NotImplementedError('users must define get_recommendation to use this base class')



    @abc.abstractmethod
    def run_ranker(self, nextevent):
        raise NotImplementedError('users must define run_ranker to use this base class')

    def add_score(self, nextevent):
        publisher = nextevent[self.publisher_id_idx]
        if self.true_rec(nextevent) in self.get_recommendation(nextevent):
            self.evaluation.add_correct_site(publisher)
        else:
            self.evaluation.add_incorrect_site(publisher)

    def run_test(self):
        self.bridge_table = open(self.BASEDIR + 'CSV/bridge-tables/test_real_bridge', 'rt')
        self.rec_csv = gzip.open(self.BASEDIR + 'CSV/recs/rec_1_csv.gz','rt')
        self.event_csv = open(self.BASEDIR  + 'CSV/events/event_1.csv', 'rt')
        self.update_csv = open(self.BASEDIR  + 'CSV/updates/item_update_1.csv', 'rt')
        self.read_header(self.bridge_table)
        self.read_header(self.event_csv)
        self.read_header(self.rec_csv)
        self.run_ranker()

    def run_day(self, day):
        self.bridge_table = gzip.open(self.BASEDIR + 'CSV/bridge-tables/bridge_table_%s_csv.gz' % day, 'rt')
        self.rec_csv = gzip.open(self.BASEDIR + 'CSV/recs/rec_%s_csv.gz' % day,'rt')
        self.event_csv = open(self.BASEDIR  + 'CSV/events/event_%s.csv' % day, 'rt')
        self.update_csv = open(self.BASEDIR  + 'CSV/updates/item_update_%s.csv' % day, 'rt')
        self.read_header(self.bridge_table)
        self.read_header(self.event_csv)
        self.read_header(self.rec_csv)
        self.run_ranker()

    def read_header(self, csv):
        next(csv)