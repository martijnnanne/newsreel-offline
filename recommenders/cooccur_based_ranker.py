from collections import OrderedDict

from mapping import Mapping
from recommenders.generic_recommender import  GenericRecommender, Stats


class CooccurRank(GenericRecommender):
    def __init__(self, BASEDIR):
        super().__init__(BASEDIR)
        self.name = 'coocrank'

        mapper = Mapping()
        self.rec_mapping = mapper.get_header_rec()
        self.event_mapping = mapper.get_header_event()
        self.item_id_idx = self.rec_mapping.index('ITEM_SOURCE')
        self.publisher_id_idx = self.rec_mapping.index('PUBLISHER')
        self.recs_idx = self.event_mapping.index('recs')
        self.user_id_idx = self.event_mapping.index('USER_COOKIE')

        self.user_item_dict = {}
        self.cooccur_dict = {}

        self.correct = 0
        self.total_events = 0
        self.nrrows =0

    def init_new_day(self):
        self.evaluation = Stats()
        self.user_item_dict = {}
        self.cooccur_dict = {}

    def store(self, item_id, user_id ):
        if not user_id in self.user_item_dict.keys():
            self.user_item_dict[user_id] = []
        if item_id == 0 or item_id =='0' or user_id == '0' or user_id == 0:
            return

        if item_id not in self.cooccur_dict.keys():
            self.cooccur_dict[item_id] = {}
        for coitem in self.user_item_dict[user_id]:
            if coitem not in self.cooccur_dict.keys():
                self.cooccur_dict[coitem] = {}
            if coitem in self.cooccur_dict[item_id].keys():
                self.cooccur_dict[item_id][coitem] += 1
            else:
                self.cooccur_dict[item_id][coitem] = 1
            if item_id in self.cooccur_dict[coitem].keys():
                self.cooccur_dict[coitem][item_id] += 1
            else:
                self.cooccur_dict[coitem][item_id] = 1
        if not item_id in self.user_item_dict[user_id]:
            self.user_item_dict[user_id].append(item_id)

    def store_view(self, nextrec):
        try:
            item_id = nextrec[self.item_id_idx]
            user_id = nextrec[self.user_id_idx]

            self.store(item_id, user_id)
        except:
            print("exception")
            print(nextrec)
            print(self.nrrows)


    def store_event(self, nextevent):
        user_id = nextevent[self.user_id_idx]
        rec_clicked = self.true_rec(nextevent)
        self.store_view(nextevent)

        if user_id != '0':
            self.store(item_id=rec_clicked, user_id=user_id)
        else:
            item_id = nextevent[self.item_id_idx]
            if item_id not in self.cooccur_dict.keys():
                self.cooccur_dict[item_id] = {}
            if rec_clicked not in self.cooccur_dict.keys():
                self.cooccur_dict[rec_clicked] = {}
            if rec_clicked in self.cooccur_dict[item_id].keys():
                self.cooccur_dict[item_id][rec_clicked] += 1
            else:
                self.cooccur_dict[item_id][rec_clicked] = 1
            if item_id in self.cooccur_dict[rec_clicked].keys():
                self.cooccur_dict[rec_clicked][item_id] += 1
            else:
                self.cooccur_dict[rec_clicked][item_id] = 1

    def get_recommendation(self, nextevent):
        item_id = nextevent[self.item_id_idx]
        user_id = nextevent[self.user_id_idx]
        sorted_item_list = []
        try:
            item_dict = self.cooccur_dict[item_id]
            ordered = OrderedDict(sorted(item_dict.items(),key=lambda t: t[1], reverse=True))
            sorted_item_list = list(ordered.keys())
        except:
            pass
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
            if(command == 'event'):
                self.total_events += 1
                nextevent = self.event_csv.readline().split('\t')
                self.add_score(nextevent)
                self.store_event(nextevent)

            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                self.logging()