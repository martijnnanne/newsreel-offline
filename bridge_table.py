import csv
import gzip


from mapping import Mapping
import json

class Bridge_table:

    def __init__(self):
        self.idx_event = 0
        self.idx_update = 0
        self.idx_rec = 0
        self.nrrows = 0

        self.mapper = Mapping()

    def parse(self, infile, outfile):
        file = gzip.open('Data/Newsreel/%s' % infile, 'rt')
        bridge_csv = csv.writer(gzip.open('CSV/%s' % outfile, 'wt'), delimiter="\t")
        bridge_csv.writerow(['type', 'idx'])
        for line in file:
            splitline = line.split('\t')
            command = splitline[0]
            if (command == 'recommendation_request'):
                bridge_csv.writerow(['rec', self.idx_rec])
                self.idx_rec +=1
            elif (command == 'event_notification'):
                bridge_csv.writerow(['event', self.idx_event])
                self.idx_event +=1
            elif (command == 'item_update'):
                bridge_csv.writerow(['update', self.idx_update])
                self.idx_update +=1
            else:
                print('something went wrong')
                print(splitline)
            self.nrrows += 1
            if (self.nrrows % 100000 == 0):
                print(self.nrrows)


    def store_limit(self, json_split, arr):
        try:
            for key in self.mapper.limit:
                try:
                    arr.append(json_split[key])
                except:
                    arr.append('')
        except:
            pass

    def store_limit_event(self, json_split, arr):
        try:
            for key in self.mapper.recs:
                try:
                    arr.append(json_split[key]['ints'])
                except:
                    arr.append('')
        except:
            pass

    def getJson(self, line):
        json_str = line.rsplit(',', 2)[0]
        json_obj = json.loads(json_str)
        return json_obj

    # def append_limit(self, infile, outfile, event_csv):
    #     file = gzip.open('Data/Newsreel/%s' % infile, 'rt')
    #     old_rec_csv = gzip.open('CSV/recs/%s' % outfile,'rt')
    #     new_rec_csv = csv.writer(gzip.open('CSV/%s' % outfile, 'wt'), delimiter="\t")
    #
    #
    #     for line in file:
    #         splitline = line.split('\t')
    #         command = splitline[0]
    #         json_split = self.getJson(splitline[1])
    #         if (command == 'recommendation_request'):
    #             arr = old_rec_csv.readline().split('\t')
    #             arr[-1] = arr[-1].strip()
    #             self.store_limit(json_split, arr)
    #             new_rec_csv.writerow(arr)
    #         elif (command == 'event_notification'):
    #             pass
    #             # arr = old_event_csv.readline().split('\t')
    #             # arr[-1] = arr[-1].strip()
    #             # self.store_limit_event(json_split,arr)
    #             # new_event_csv.writerow(arr)
    #         elif (command == 'item_update'):
    #             pass
    #         else:
    #             print('something went wrong')
    #             print(splitline)
    #         self.nrrows += 1
    #         if (self.nrrows % 100000 == 0):
    #             print(self.nrrows)


    def run_parser(self, outfile):
        for i in range(1, 28):
            if (i < 10):
                self.parse('nr2016-02-0%s.log.gz' % i, '%s_%s_csv.gz' % ('rec',i))
            else:
                self.parse('nr2016-02-%s.log.gz' % i, '%s_%s_csv.gz' % ('rec', i))

    def run_append_limit(self):
        for i in range(1, 28):
            event_csv = csv.writer(open('CSV/event_%s.csv' % i, 'w'), delimiter="\t")
            if (i < 10):
                self.append_limit('nr2016-02-0%s.log.gz' % i, '%s_%s_csv.gz' % ('rec',i), event_csv)
            else:
                self.append_limit('nr2016-02-%s.log.gz' % i, '%s_%s_csv.gz' % ('rec', i), event_csv)

    def run_test(self):
        self.parse('1ktest.log', 'bridge_table_test.gz')

    def run_limit(self):
        self.append_limit('1ktest.log','test_rec', csv.writer(open('test_csv', 'w'), delimiter="\t"))

b = Bridge_table()
b.run_append_limit()