import json
from collections import OrderedDict, Counter

from mapping import Mapping
from recommenders.generic_recommender import GenericRecommender, Stats


# keep dictionary per domain of items:views
# if item does not exist, add to the dictionary
#


class KeywordRecommender(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'keyword_rec'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.user_last_item_dict = {}
        self.item_sequence_dict = {}
        self.user_item_dict = {}
        self.keyword_dict = {}

        self.correct = 0
        self.total_events = 0
        self.nrrows =0

    def init_new_day(self):
        self.evaluation = Stats()
        self.item_sequence_dict = {}
        self.session_length = {}
        self.keyword_dict = {}


    def store_view(self, nextrec):
        try:
            publisher = nextrec[self.publisher_id_idx]
            item_id = nextrec[self.item_id_idx]
            user_id = nextrec[self.user_id_idx]
            keywords = nextrec[self.keyword_idx]
            keywords = eval(keywords)
        except:
            return

        if(not user_id in self.user_item_dict and user_id != '0'):
            self.user_item_dict[user_id] = [item_id]
        elif user_id != '0':
            self.user_item_dict[user_id].append(item_id)

        if(not publisher in self.keyword_dict):
            self.keyword_dict[publisher] = {}
        for key in keywords:
            if not key in self.keyword_dict[publisher]:
                self.keyword_dict[publisher][key] = {}
            if not item_id in self.keyword_dict[publisher][key]:
                self.keyword_dict[publisher][key][item_id] = keywords[key]




    def store_event(self, nextevent):
        last_item = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        rec_clicked = self.true_rec(nextevent)
        publisher = nextevent[self.publisher_id_idx]
        self.store_view(nextevent)

    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        sorted_item_list = []
        try:
            keywords = nextevent[self.keyword_idx]
            keywords = eval(keywords)
        except:
            return []
        sim_items = []
        for key in keywords:
            try:
                item_dict = self.keyword_dict[publisher][key]
                sim_items = sim_items + list(item_dict.keys())
            except KeyError:
                pass
        ordered = OrderedDict(sorted(Counter(sim_items).items(), key=lambda t: t[1], reverse=True))
        sorted_item_list = list(ordered.keys())

        if 0 in sorted_item_list:
            sorted_item_list.remove(0)
        if item_id in sorted_item_list:
            sorted_item_list.remove(item_id)
        try:
            user_items = self.user_item_dict[user_id]
            result = [x for x in sorted_item_list if x not in user_items]
            return result[0:6]
        except:
            return sorted_item_list[0:6]


    def run_ranker(self):
        for line in self.bridge_table:
            command = line.split('\t')[0]
            if(command == 'rec'):
                nextrec = self.rec_csv.readline().split('\t')
                self.store_view(nextrec)
                self.add_session(nextrec)
            if(command == 'event'):
                self.total_events += 1
                nextevent = self.event_csv.readline().split('\t')
                self.add_score(nextevent)
                self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()