import copy
from collections import Counter
from collections import OrderedDict

from evaluation_stats import Stats
import re
from abc import ABCMeta
import abc

import gzip

from mapping import Mapping
import numpy as np


class GenericRecommender(metaclass=ABCMeta):
    def __init__(self, BASEDIR, session_only = False, cycle_time=1, only_clicked=False):
        self.only_clicked = only_clicked
        self.BASEDIR = BASEDIR
        self.session_only = session_only
        self.evaluation = Stats()
        mapper = Mapping()
        self.publishers = mapper.publishers
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.limit_idx = self.rec_mapping.index('limit')
        self.time_idx = self.rec_mapping.index('TIME_HOUR')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.session_length = {}

        self.cycle_time = cycle_time
        self.times_changed_hour = 0
        self.time_hour = 0

        self.current_timestamp = 1456613999960
        self.prev_stats = {35774:{}, 1677:{}, 13554:{}, 418:{}, 694:{}, 3336:{}, 2522:{}}
        self.timestamps = {}

        self.click_ranking = {'35774':{}, '1677':{}, '13554':{}, '418':{}, '694':{}, '3336':{}, '2522':{}}
        self.highest_newcomer = {'35774':'', '1677':'', '13554':'', '418':'', '694':'', '3336':'', '2522':''}

        self.highest_risers = {'35774': [], '1677': [], '13554': [], '418': [], '694': [], '3336': [], '2522': []}

    def init_new_day(self):
        self.evaluation = Stats()
        self.session_length = {}



    def true_rec(self, nextevent):
        true_rec_json = nextevent[self.recs_idx]
        return re.search(r"\[([A-Za-z0-9_]+)\]", true_rec_json).group(1)

    def add_session(self, nextrec):
        try:
            item_id = nextrec[self.item_id_idx]
            user_id = nextrec[self.user_id_idx]
        except:
            return
        if(not user_id in self.session_length and user_id != '0'):
            self.session_length[user_id] = [item_id]
        elif user_id != '0':
            self.session_length[user_id].append(item_id)

    def check_cycle_time(self, stats):
        for publisher in self.publishers:
            try:
                stats_next = Counter(stats[str(publisher)])
                stats_prev = Counter(self.prev_stats[publisher])
                currentstats = Counter(stats[str(publisher)])
            except KeyError:
                print(publisher)
                return

            currentstats.subtract(stats_prev)
            ordered_next = OrderedDict(sorted(stats_next.items(), key=lambda t: t[1], reverse=True))
            ordered_current = OrderedDict(sorted(currentstats.items(), key=lambda t: t[1], reverse=True))
            intersect_keys = (stats_next & stats_prev).keys()

            # REMOVE ALL BUT TOP 250 ITEMS
            for key in intersect_keys:
                try:
                    if list(ordered_next.keys()).index(key)-list(ordered_current.keys()).index(key) < -5:
                        del stats[str(publisher)][key]
                    elif list(ordered_next.keys()).index(key) > 250:
                        del stats[str(publisher)][key]
                except KeyError:
                    print('not in list')
            try:
                self.prev_stats[publisher] = copy.deepcopy(stats_next)
            except KeyError:
                print(publisher)
                print("something else")


    def add_click_ranking(self, nextevent):
        try:
            publisher = nextevent[self.publisher_id_idx]
            item_id = self.true_rec(nextevent)
        except KeyError:
            print('KEYERROR')
            return
        if item_id in self.click_ranking[publisher]:
            self.click_ranking[publisher][item_id] += 1
        else:
            self.click_ranking[publisher][item_id] = 1

    def get_ordered_click_list(self, publisher):
        click_list = self.click_ranking[publisher]
        return list(OrderedDict(sorted(click_list.items(), key=lambda t: t[1], reverse=True)).keys())

    def get_amount_clicked(self,publisher, item_id):
        return self.click_ranking[publisher][item_id]


    def add_timestamp(self, nextevent):
        rec_clicked = self.true_rec(nextevent)
        nexttime = int(nextevent[-2])
        if rec_clicked not in self.timestamps:
            self.timestamps[rec_clicked] = nexttime
        self.current_timestamp = nexttime


    @abc.abstractmethod
    def get_recommendation(self, nextevent):
        raise NotImplementedError('users must define get_recommendation to use this base class')


    @abc.abstractmethod
    def run_ranker(self, nextevent):
        raise NotImplementedError('users must define run_ranker to use this base class')

    def add_score(self, nextevent):
        try:
            self.add_click_ranking(nextevent)
        except:
            pass
        if self.session_only:
            user_id = nextevent[self.user_id_idx]
            try:
                if len(self.make_unique(self.session_length[user_id])) > 1:
                    try:
                        publisher = nextevent[self.publisher_id_idx]
                        true_rec = self.true_rec(nextevent)
                        if true_rec in self.get_recommendation(nextevent):
                            self.evaluation.add_correct_site(publisher, true_rec, self.get_ordered_click_list(publisher), self.get_amount_clicked(publisher,true_rec))
                            return True
                        else:
                            self.evaluation.add_incorrect_site(publisher, true_rec)
                    except:
                        print('WTF')
            except KeyError:
                pass
        else:
            # try:
            publisher = nextevent[self.publisher_id_idx]
            true_rec = self.true_rec(nextevent)
            if true_rec in self.get_recommendation(nextevent):
                self.evaluation.add_correct_site(publisher,true_rec, self.get_ordered_click_list(publisher), self.get_amount_clicked(publisher,true_rec))
                return True
            else:
                self.evaluation.add_incorrect_site(publisher, true_rec)
                return False
            # except Exception:
            #     print(Exception)

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
        print('number rows progress' , self.nrrows)
        try:
            print('precision', self.evaluation.total_correct_all / self.evaluation.total_count_all)
            print('recall', len(self.evaluation.sites_correct)/len(self.evaluation.total_sites))
            print('cumalative gain tagesspiegel', self.evaluation.CG('35774'))
            print('avg gain tagesspiegel', self.evaluation.avgCG('35774'))
        except:
            print("division by zero")
