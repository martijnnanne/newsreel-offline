
# for each site, count correct/not correct
# for each day, count correct/not correct
# for each day, for each website, count correct/notcorrect

class Stats():
    def __init__(self):
        self.stats_site = {'35774': 0, '1677': 0, '13554': 0, '418': 0, '694': 0, '3336': 0, '2522': 0}
        self.total_count_site = {'35774': 0, '1677': 0, '13554': 0, '418': 0, '694': 0, '3336': 0, '2522': 0}

        self.correct_day = {}
        self.total_day = {}
        self.currentday = 0


    def add_correct_site(self, publisher_id):
        self.stats_site[publisher_id] += 1
        self.total_count_site[publisher_id] +=1

    def add_incorrect_site(self, publisher_id):
        self.total_count_site[publisher_id] += 1

