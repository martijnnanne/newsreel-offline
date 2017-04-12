
import gzip
import csv
from mapping import Mapping
from collections import OrderedDict
import re
import time
from nltk.stem.snowball import GermanStemmer
from generic_recommender import GenericRecommender
from nltk.corpus import stopwords

class ContentRank(GenericRecommender):
    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'contentrank'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.update_mapping = mapper.get_header_update()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.limit_idx = self.rec_mapping.index('limit')
        self.title_idx = self.update_mapping.index('title')
        self.text_idx = self.update_mapping.index('text')
        self.update_id_idx = self.update_mapping.index('id')
        self.update_domainid_idx  = self.update_mapping.index('domainid')

        self.germanStemmer = GermanStemmer(ignore_stopwords=True)
        self.stopwords = stopwords.words('german')
        self.stems = {} # (item, [stem, stem, stem])

        self.correct = 0
        self.total_events = 0
        self.nrrows =0

        self.counts = {}




    def get_recommendation(self, nextevent):
        publisher_id = nextevent[self.publisher_id_idx]
        item_id = nextevent[self.item_id_idx]
        overlap_dict = {}
        try:
            stems_current_item = self.stems[publisher_id][item_id]
            for key in self.stems[publisher_id].keys():
                if not key == item_id:
                    overlap = len([w1 for w1 in self.stems[publisher_id][key] if w1 in stems_current_item])
                    overlap_dict[key] = overlap
        except:
            pass
        ordered = OrderedDict(sorted(overlap_dict.items(),key=lambda t: t[1], reverse=True))
        sorted_item_list = list(ordered.keys())
        return sorted_item_list[0:6]

    def store_update(self, nextupdate):
        title = nextupdate[self.title_idx]
        text = nextupdate[self.text_idx]
        item_id = nextupdate[self.update_id_idx]
        publisher_id = nextupdate[self.update_domainid_idx]
        fulltext = (title + " " + text).split()
        fulltext = [self.germanStemmer.stem(word) for word in fulltext if word not in self.stopwords]
        if publisher_id not in self.stems.keys():
            self.stems[publisher_id] = {}
        self.stems[publisher_id][item_id] = fulltext


    def run_ranker(self):
        for line in self.bridge_table:
            command = line.split('\t')[0]
            if(command == 'event'):
                self.total_events += 1
                nextevent = self.event_csv.readline().split('\t')
                self.add_score(nextevent)
            if(command == 'update'):
                nextupdate = self.update_csv.readline().split('\t')
                self.store_update(nextupdate)
            self.nrrows += 1
            if (self.nrrows % 1000000 == 0):
                print(self.nrrows)

