
# for each site, count correct/not correct
# for each day, count correct/not correct
# for each day, for each website, count correct/notcorrect
import math
class Stats():
    def __init__(self):
        self.stats_site = {'35774': 0, '1677': 0, '13554': 0, '418': 0, '694': 0, '3336': 0, '2522': 0, '596':0}
        self.total_count_site = {'35774': 0, '1677': 0, '13554': 0, '418': 0, '694': 0, '3336': 0, '2522': 0, '596':0}

        self.correct_day = {}
        self.total_day = {}
        self.currentday = 0

        self.total_correct_all = 0
        self.total_count_all  = 0

        self.sites_correct = []
        self.total_sites = []

        self.recall_site = {'35774': [], '1677': [], '13554': [], '418': [], '694': [], '3336': [], '2522': [], '596':[]}
        self.total_recall_site = {'35774': [], '1677': [], '13554': [], '418': [], '694': [], '3336': [], '2522': [], '596':[]}

        self.relevances = {'35774': [], '1677': [], '13554': [], '418': [], '694': [], '3336': [], '2522': [], '596':[]}


    def add_correct_site(self, publisher_id, true_rec, ordered_click_list, times_clicked):
        self.stats_site[publisher_id] += 1
        self.total_count_site[publisher_id] +=1

        self.total_correct_all += 1
        self.total_count_all += 1

        if true_rec not in self.recall_site[publisher_id]:
            self.recall_site[publisher_id].append(true_rec)
            self.sites_correct.append(true_rec)
            self.total_sites.append(true_rec)
        if true_rec not in self.total_recall_site[publisher_id]:
            self.total_recall_site[publisher_id].append(true_rec)
        # print(self.get_relevance(true_rec, ordered_click_list, times_clicked))
        self.relevances[publisher_id].append(self.get_relevance(true_rec, ordered_click_list, times_clicked))


    def add_incorrect_site(self, publisher_id, true_rec):
        self.total_count_site[publisher_id] += 1
        self.total_count_all += 1

        if true_rec not in self.total_recall_site[publisher_id]:
            self.total_recall_site[publisher_id].append(true_rec)
            self.total_sites.append(true_rec)

    def CG(self, publisher):
        cg = 0
        for relevance in self.relevances[publisher]:
            cg += relevance
        return cg

    def avgCG(self,publisher):
        cg = 0
        i = 0
        for relevance in self.relevances[publisher]:
            i+=1
            cg += relevance
        return cg/i

    def get_relevance(self, item_id, ordered_click_list, times_clicked):
        return 1/math.sqrt(times_clicked)
        # return ordered_click_list.index(item_id)+1 / len(ordered_click_list)

        # if(ordered_click_list.index(item_id)) < len(ordered_click_list)/8:
        #     return 1
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 2:
        #     return 2
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 3:
        #     return 3
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 4:
        #     return 4
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 5:
        #     return 5
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 6:
        #     return 6
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 7:
        #     return 7
        # elif(ordered_click_list.index(item_id)) < len(ordered_click_list)/8 * 8:
        #     return 8