from collections import OrderedDict

from evaluation_stats import Stats
from mapping import Mapping
from recommenders.generic_recommender import GenericRecommender

from datetime import datetime

# keep dictionary per domain of items:views
# if item does not exist, add to the dictionary
#


class PopRankEvent(GenericRecommender):
    def __init__(self, BASEDIR, flushing=False, flush_cycle=24):
        super().__init__(BASEDIR)
        self.flushing = flushing
        self.flush_cycle = flush_cycle
        if flushing:
            self.name = 'poprank_event_%s' % flush_cycle
        else:
            self.name = 'poprank_event'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.limit_idx = self.rec_mapping.index('limit')
        self.time_idx = self.rec_mapping.index('TIME_HOUR')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')

        self.popdict = OrderedDict()
        self.user_item_dict = {}

        self.correct = 0
        self.total_events = 0
        self.nrrows =0





    def flush_popdict(self):
        publishers = [key for key in self.popdict.keys()]
        for publisher in publishers:
            publisher_dict = self.popdict[publisher]
            ordered = list(OrderedDict(sorted(publisher_dict.items(), key=lambda t: t[1], reverse=True)).keys())
            len_ordered = len(ordered)
            index = 0
            for key in ordered:
                if index > 200:
                    del self.popdict[publisher][key]
                index +=1


    def init_new_day(self):
        if not self.flushing:
            self.popdict = OrderedDict()
        self.evaluation = Stats()
        self.session_length = {}

    def store_view(self, nextrec, rec=None):
        try:
            publisher = nextrec[self.publisher_id_idx]
            item_id = nextrec[self.item_id_idx]
            user_id = nextrec[self.user_id_idx]
        except:
            return

        # try:
        #     if item_id in self.user_item_dict[user_id]:
        #         return
        # except:
        #     pass

        if(not user_id in self.user_item_dict and user_id != '0'):
            self.user_item_dict[user_id] = [item_id]
        elif user_id != '0':
            self.user_item_dict[user_id].append(item_id)

        if(not publisher in self.popdict):
            self.popdict[publisher] = {}
        # if(not item_id in self.popdict[publisher]):
        #     self.popdict[publisher][item_id] = 1
        # else:
        #     self.popdict[publisher][item_id] += 1

        if rec:
            if rec not in self.popdict[publisher]:
                self.popdict[publisher][rec] = 1
            else:
                self.popdict[publisher][rec] += 1
            if user_id != '0':
                self.user_item_dict[user_id].append(rec)


    def get_recommendation(self, nextevent):
        publisher = nextevent[self.publisher_id_idx]
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        try:
            publisher_dict = self.popdict[publisher]
        except:
            return []
        ordered = OrderedDict(sorted(publisher_dict.items(),key=lambda t: t[1], reverse=True))
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
                self.add_timestamp(nextevent)
                self.store_view(nextevent, self.true_rec(nextevent))
                nexttime = int(nextevent[-2])
                nexttime = datetime.fromtimestamp(int(nexttime)/1000).hour
                if self.time_hour is not nexttime and self.flushing:
                    self.times_changed_hour += 1
                    if self.times_changed_hour % self.flush_cycle == 0:
                        self.check_cycle_time(self.popdict)
                    self.time_hour = nexttime
            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                self.logging()








