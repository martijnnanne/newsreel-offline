
# for each site, count correct/not correct
# for each day, count correct/not correct
# for each day, for each website, count correct/notcorrect

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


    def add_correct_site(self, publisher_id, true_rec):
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


    def add_incorrect_site(self, publisher_id, true_rec):
        self.total_count_site[publisher_id] += 1
        self.total_count_all += 1

        if true_rec not in self.total_recall_site[publisher_id]:
            self.total_recall_site[publisher_id].append(true_rec)
            self.total_sites.append(true_rec)
