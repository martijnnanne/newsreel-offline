
import gzip
import csv
from mapping import Mapping
from collections import OrderedDict
import re
import time
from evaluation_stats import Stats
from generic_recommender import GenericRecommender
# keep dictionary per domain of items:views
# if item does not exist, add to the dictionary
#


class PopRank(GenericRecommender):
    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'poprank'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.limit_idx = self.rec_mapping.index('limit')

        self.popdict = OrderedDict()

        self.correct = 0
        self.total_events = 0
        self.nrrows =0



    def init_new_day(self):
        self.popdict = OrderedDict()
        self.evaluation = Stats()

    def store_view(self, nextrec, rec=None):
        try:
            publisher = nextrec[self.publisher_id_idx]
            item_id = nextrec[self.item_id_idx]
        except:
            return

        if(not publisher in self.popdict):
            self.popdict[publisher] = {}
        if(not item_id in self.popdict[publisher]):
            self.popdict[publisher][item_id] = 1
        else:
            self.popdict[publisher][item_id] += 1

        if rec:
            if rec not in self.popdict[publisher]:
                self.popdict[publisher][rec] = 1
            else:
                self.popdict[publisher][rec] += 1

    def get_recommendation(self, nextevent):
        publisher = nextevent[self.publisher_id_idx]
        item_id = nextevent[self.item_id_idx]
        try:
            publisher_dict = self.popdict[publisher]
        except:
            return []
        ordered = OrderedDict(sorted(publisher_dict.items(),key=lambda t: t[1], reverse=True))
        sorted_item_list = list(ordered.keys())
        # sorted_item_list = []
        if 0 in sorted_item_list:
            sorted_item_list.remove(0)
        if item_id in sorted_item_list:
            sorted_item_list.remove(item_id)

        return sorted_item_list[0:6]



    def run_ranker(self):
        for line in self.bridge_table:
            command = line.split('\t')[0]
            if(command == 'rec'):
                nextrec = self.rec_csv.readline().split('\t')
                self.store_view(nextrec)
            if(command == 'event'):
                self.total_events += 1
                nextevent = self.event_csv.readline().split('\t')
                self.add_score(nextevent)
                self.store_view(nextevent, self.true_rec(nextevent))

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)








