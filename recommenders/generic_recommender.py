
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
        try:
            publisher = nextevent[self.publisher_id_idx]
            true_rec = self.true_rec(nextevent)
            if true_rec in self.get_recommendation(nextevent):
                self.evaluation.add_correct_site(publisher,true_rec)
            else:
                self.evaluation.add_incorrect_site(publisher, true_rec)
        except:
            print('WTF')

    def run_test(self):
        self.bridge_table = open(self.BASEDIR + 'bridge-tables/test_real_bridge', 'rt')
        self.rec_csv = gzip.open(self.BASEDIR + 'recs/rec_1_csv.gz','rt')
        self.event_csv = open(self.BASEDIR  + 'events/event_1.csv', 'rt')
        self.update_csv = open(self.BASEDIR  + 'updates/item_update_1.csv', 'rt')
        self.read_header(self.bridge_table)
        self.read_header(self.event_csv)
        self.read_header(self.rec_csv)
        self.run_ranker()

    def run_day(self, day):
        self.bridge_table = gzip.open(self.BASEDIR + 'bridge-tables/bridge_table_%s_csv.gz' % day, 'rt')
        self.rec_csv = gzip.open(self.BASEDIR + 'recs/rec_%s_csv.gz' % day,'rt')
        self.event_csv = open(self.BASEDIR  + 'events/event_%s.csv' % day, 'rt')
        self.update_csv = open(self.BASEDIR  + 'updates/item_update_%s.csv' % day, 'rt')
        self.read_header(self.bridge_table)
        self.read_header(self.event_csv)
        self.read_header(self.rec_csv)
        self.run_ranker()

    def read_header(self, csv):
        next(csv)

    def make_unique(self, user_items):
        seen = set()
        seen_add = seen.add
        user_items = [x for x in user_items if not (x in seen or seen_add(x))]
        return user_items

    def logging(self):
        print(self.nrrows)
        print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
        print(len(self.evaluation.sites_correct)/len(self.evaluation.total_sites))