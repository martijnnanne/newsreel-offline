from mapping import Mapping
from collections import OrderedDict, Counter
import re
from generic_recommender import GenericRecommender

class SessionSeqRank(GenericRecommender):

    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'session_seqrank'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')

        self.user_item_dict = {}
        self.item_sequence_dict = {}

        self.correct = 0
        self.total_events = 0
        self.nrrows =0




    def store_view(self, nextrec):
        try:
            publisher = nextrec[self.publisher_id_idx]
            item_id = nextrec[self.item_id_idx]
            user_id = nextrec[self.user_id_idx]
        except:
            print(nextrec)
            return

        if user_id == '0':
            return

        if(not user_id in self.user_item_dict):
            self.user_item_dict[user_id] = [item_id]
        else:
            last_item = self.user_item_dict[user_id][-1]
            if last_item not in self.item_sequence_dict:
                self.item_sequence_dict[last_item] = {}
            if item_id in self.item_sequence_dict[last_item]:
                self.item_sequence_dict[last_item][item_id] += 1
            else:
                self.item_sequence_dict[last_item][item_id] = 1
            self.user_item_dict[user_id].append(item_id)

    def store_event(self, nextevent):
        last_item = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        rec_clicked = self.true_rec(nextevent)

        if user_id not in self.user_item_dict and user_id is not '0':
            self.user_item_dict[user_id] = [rec_clicked]
        elif user_id in self.user_item_dict and user_id is not '0':
            self.user_item_dict[user_id].append(rec_clicked)
        if last_item not in self.item_sequence_dict:
            self.item_sequence_dict[last_item] = {}
        if rec_clicked in self.item_sequence_dict[last_item]:
            self.item_sequence_dict[last_item][rec_clicked] += 1
        else:
            self.item_sequence_dict[last_item][rec_clicked] = 1

    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        sorted_item_list = []
        try:
            user_items = self.user_item_dict[user_id]
            count_dict = Counter(self.item_sequence_dict[item_id])
            for item in reversed(user_items):
                item_dict = Counter(self.item_sequence_dict[item])
                count_dict = count_dict + item_dict
            ordered = OrderedDict(sorted(count_dict.items(),key=lambda t: t[1], reverse=True))
            sorted_item_list = list(ordered.keys())
        except KeyError:
            try:
                mpc_dict = self.item_sequence_dict[item_id]
                ordered = OrderedDict(sorted(mpc_dict.items(),key=lambda t: t[1], reverse=True))
                sorted_item_list = list(ordered.keys())
            except:
                sorted_item_list = []

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
                self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 1000000 == 0):
                print(self.nrrows)
                print(self.evaluation.total_correct_all / self.evaluation.total_count_all)