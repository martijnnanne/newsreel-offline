import json
import random
from collections import OrderedDict, Counter
from datetime import datetime

from mapping import Mapping
from recommenders.generic_recommender import GenericRecommender, Stats
from collections import OrderedDict, Counter

from mapping import Mapping
from recommenders.generic_recommender import GenericRecommender, Stats
from recommenders.keyword_recommender import KeywordRecommender
from recommenders.mpc_session_event_only import MPCEventSession
from recommenders.mpc_session_recommender import SessionSeqRank
from recommenders.poprank_event import PopRankEvent
from recommenders.popular_based_recommender import PopRank

# keep dictionary per domain of items:views
# if item does not exist, add to the dictionary
#


class PopMpcHybrid(GenericRecommender):

    def __init__(self, BASEDIR, only_clicked=False):
        super().__init__(BASEDIR)
        self.name = 'popevent_mpcexplore'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.poprankevent = PopRankEvent(BASEDIR)
        self.mpc_event_session = MPCEventSession(BASEDIR)

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
        self.poprankevent.init_new_day()
        self.mpc_event_session.init_new_day()


    def store_view(self, nextrec):
        self.poprankevent.store_view(nextrec)
        self.mpc_event_session.store_view(nextrec)


    def store_event(self, nextevent):
        self.poprankevent.store_view(nextevent, self.true_rec(nextevent))
        self.mpc_event_session.store_event(nextevent)


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_poprank = self.poprankevent.get_recommendation(nextevent)
        recs_mpcevent = []
        recs_mpcevent = self.mpc_event_session.get_recommendation(nextevent)
        # print(recs_mpcevent[0:4], recs_keyword[0:2])

        if len(recs_poprank) < 6:
            return recs_mpcevent
        sorted_item_list = []
        for i in range(0,6):
            try:
                if random.randint(0, 100) < 20:
                    sorted_item_list.append(recs_mpcevent[i])
                else:
                    sorted_item_list.append(recs_poprank[i])
            except IndexError:
                pass
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
                result = self.add_score(nextevent)
                if self.only_clicked and result:
                    self.store_event(nextevent)
                elif not self.only_clicked:
                    self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()



class PopPophybrid(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'popevent_popularityexplore'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.main = PopRankEvent(BASEDIR)
        self.off = PopRank(BASEDIR)

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
        self.main.init_new_day()
        self.off.init_new_day()


    def store_view(self, nextrec):
        self.main.store_view(nextrec)
        self.off.store_view(nextrec)


    def store_event(self, nextevent):
        self.main.store_view(nextevent, self.true_rec(nextevent))
        self.off.store_view(nextevent, self.true_rec(nextevent))


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_main = self.main.get_recommendation(nextevent)
        recs_mpcevent = []
        recs_mpcevent = self.off.get_recommendation(nextevent)
        # print(recs_mpcevent[0:4], recs_keyword[0:2])
        sorted_item_list = []

        if len(recs_main) < 6:
            # print(recs_main)
            return recs_mpcevent

        for i in range(0,6):
            try:
                if random.randint(0, 100) < 20:
                    sorted_item_list.append(recs_mpcevent[i])
                else:
                    sorted_item_list.append(recs_main[i])
            except IndexError:
                pass
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
                result = self.add_score(nextevent)
                if self.only_clicked and result:
                    self.store_event(nextevent)
                elif not self.only_clicked:
                    self.store_event(nextevent)
                nexttime = int(nextevent[-2])
                nexttime = datetime.fromtimestamp(int(nexttime) / 1000).hour
                if self.time_hour is not nexttime:
                    self.times_changed_hour += 1
                    if self.times_changed_hour % 1 == 0:
                        # self.popdict = {}
                        self.off.flush_popdict()
                        self.time_hour = nexttime

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()

class PopKeyHybrid(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'popevent_keyword'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.main = PopRankEvent(BASEDIR)
        self.off = KeywordRecommender(BASEDIR)

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
        self.main.init_new_day()
        self.off.init_new_day()


    def store_view(self, nextrec):
        self.main.store_view(nextrec)
        self.off.store_view(nextrec)


    def store_event(self, nextevent):
        self.main.store_view(nextevent, self.true_rec(nextevent))
        self.off.store_event(nextevent)


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_main = self.main.get_recommendation(nextevent)
        recs_mpcevent = []
        recs_mpcevent = self.off.get_recommendation(nextevent)
        # print(recs_mpcevent[0:4], recs_keyword[0:2])
        sorted_item_list = []
        if len(recs_main) < 6:
            return recs_mpcevent

        for i in range(0,6):
            try:
                if random.randint(0, 100) < 20:
                    sorted_item_list.append(recs_mpcevent[i])
                else:
                    sorted_item_list.append(recs_main[i])
            except IndexError:
                pass
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
                result = self.add_score(nextevent)
                if self.only_clicked and result:
                    self.store_event(nextevent)
                elif not self.only_clicked:
                    self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()

class PopMpcAll(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'popevent_mpcsessionall'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.main = PopRankEvent(BASEDIR)
        self.off = SessionSeqRank(BASEDIR)

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
        self.main.init_new_day()
        self.off.init_new_day()


    def store_view(self, nextrec):
        self.main.store_view(nextrec)
        self.off.store_view(nextrec)


    def store_event(self, nextevent):
        self.main.store_view(nextevent, self.true_rec(nextevent))
        self.off.store_event(nextevent)


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_main = self.main.get_recommendation(nextevent)
        recs_mpcevent = []
        recs_mpcevent = self.off.get_recommendation(nextevent)
        # print(recs_mpcevent[0:4], recs_keyword[0:2])
        sorted_item_list = []

        if len(recs_main) < 6:
            return recs_mpcevent

        for i in range(0,6):
            try:
                if random.randint(0, 100) < 20:
                    sorted_item_list.append(recs_mpcevent[i])
                else:
                    sorted_item_list.append(recs_main[i])
            except IndexError:
                pass

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
                result = self.add_score(nextevent)
                if self.only_clicked and result:
                    self.store_event(nextevent)
                elif not self.only_clicked:
                    self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()


class MPCSOLO(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'mpc click only'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.seqrank = SessionSeqRank(BASEDIR)

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
        self.seqrank.init_new_day()


    def store_view(self, nextrec):
        self.seqrank.store_view(nextrec)


    def store_event(self, nextevent):
        self.seqrank.store_event(nextevent)


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_mpcevent = self.seqrank.get_recommendation(nextevent)
        return recs_mpcevent


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
                result = self.add_score(nextevent)
                if self.only_clicked and result:
                    self.store_event(nextevent)
                elif not self.only_clicked:
                    self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()

class POPSOLO(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'pop click only'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')
        self.keyword_idx = self.rec_mapping.index('KEYWORD')

        self.poprank = PopRank(BASEDIR)

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
        self.poprank.init_new_day()


    def store_view(self, nextrec):
        self.poprank.store_view(nextrec)


    def store_event(self, nextevent):
        self.poprank.store_view(nextevent, self.true_rec(nextevent))


    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        publisher = nextevent[self.publisher_id_idx]
        recs_mpcevent = self.poprank.get_recommendation(nextevent)
        return recs_mpcevent


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
                result = self.add_score(nextevent)
                if self.only_clicked and result:
                    self.store_event(nextevent)
                elif not self.only_clicked:
                    self.store_event(nextevent)
                nexttime = int(nextevent[-2])
                nexttime = datetime.fromtimestamp(int(nexttime) / 1000).hour
                if self.time_hour is not nexttime:
                    self.times_changed_hour += 1
                    if self.times_changed_hour % 1 == 0:
                        # self.popdict = {}
                        self.poprank.flush_popdict()
                        self.time_hour = nexttime

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)
                self.logging()