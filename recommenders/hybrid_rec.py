import json
from collections import OrderedDict, Counter

from mapping import Mapping
from recommenders.generic_recommender import GenericRecommender, Stats
from recommenders.keyword_recommender import KeywordRecommender
from recommenders.mpc_event_only import MPCEvent


# keep dictionary per domain of items:views
# if item does not exist, add to the dictionary
#


class HybridRec(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'most_popular_topic'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.keyword_rec = KeywordRecommender(BASEDIR)
        self.mpc_event = MPCEvent(BASEDIR)

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
        self.keyword_rec.init_new_day()
        self.mpc_event.init_new_day()


    def store_view(self, nextrec):
        self.keyword_rec.store_view(nextrec)
        self.mpc_event.store_view(nextrec)


    def store_event(self, nextevent):
        self.keyword_rec.store_event(nextevent)
        self.mpc_event.store_event(nextevent)


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_mpcevent = self.mpc_event.get_recommendation(nextevent)
        recs_keyword = []
        if len(recs_mpcevent) > 0:
            recs_keyword = self.keyword_rec.get_recommendation_fromitemid(nextevent, recs_mpcevent[0])
        # print(recs_mpcevent[0:4], recs_keyword[0:2])
        sorted_item_list = recs_mpcevent[0:4] + recs_keyword[0:2]
        return sorted_item_list


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