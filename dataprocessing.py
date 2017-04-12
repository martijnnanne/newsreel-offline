
import csv
import json
from mapping import Mapping
import gzip

mapper = Mapping()

# file = open('Data/Newsreel/nr2016-02-28.log','r')
# # rec_csv = csv.writer(gzip.open('CSV/rec.z','wt'), delimiter=",")
# rec_csv = csv.writer(open('CSV/rec.csv','w'), delimiter="\t")
# event_csv = csv.writer(open('CSV/event.csv', 'w'), delimiter="\t")
# item_csv = csv.writer(open('CSV/item_update.csv', 'w'), delimiter="\t")


class Processor():

    def __init__(self):
        pass


    def getJson(self,line):
        json_str = line.rsplit(',', 2)[0]
        json_obj = json.loads(json_str)
        return json_obj

    def store_simple_context(self,json_split, arr):
        simplecontext = json_split['context']['simple']

        for key in mapper.simple_keys:
            try:
                arr.append(simplecontext[str(key)])
            except:
                arr.append('')

    def store_cluster_context(self,json_split, arr):
        try:
            cluster_context = json_split['context']['clusters']
            for key in mapper.cluster_keys:
                try:
                    arr.append(cluster_context[str(key)])
                except:
                    arr.append('')
        except:
            pass

    def store_list_context(self,json_split, arr):
        try:
            list_context = json_split['context']['lists']
            for key in mapper.list_keys:
                try:
                    arr.append(list_context[str(key)])
                except:
                    arr.append('')
        except:
            pass

    def store_item_update(self,json_split, arr):
        try:
            for key in mapper.item_update_keys:
                try:
                    arr.append(json_split[key])
                except:
                    arr.append('')
        except:
            pass

    def store_recs(self, json_split, arr):
        try:
            for key in mapper.recs:
                try:
                    arr.append(json_split[key]['ints'])
                except:
                    arr.append('')
        except:
            pass

    def store_timestamp_event(self, json_split, arr):
        try:
            for key in mapper.timestamp:
                try:
                    arr.append(json_split[key])
                except:
                    arr.append('')
        except:
            pass

    def store_type_event(self, json_split, arr):
        try:
            for key in mapper.type:
                try:
                    arr.append(json_split[key])
                except:
                    arr.append('')
        except:
            pass



    def parse(self, infile, outfile,event_csv, item_csv, nrrows):
        file = gzip.open('Data/Newsreel/%s' % infile, 'rt')
        rec_csv = csv.writer(gzip.open('CSV/%s' % outfile, 'wt'), delimiter="\t")

        rec_csv.writerow(mapper.simple_header + mapper.cluster_header + mapper.list_header)
        item_csv.writerow(mapper.item_update_keys)
        event_csv.writerow(mapper.simple_header + mapper.cluster_header + mapper.list_header + mapper.recs + mapper.timestamp + mapper.type)
        i = 0
        for line in file:
            splitline = line.split('\t')
            command = splitline[0]
            json_split = self.getJson(splitline[1])
            if(command == 'recommendation_request'):
                arr = []
                self.store_simple_context(json_split, arr)
                self.store_cluster_context(json_split, arr)
                self.store_list_context(json_split, arr)
                rec_csv.writerow(arr)
            elif(command == 'event_notification'):
                arr = []
                self.store_simple_context(json_split, arr)
                self.store_cluster_context(json_split, arr)
                self.store_list_context(json_split, arr)
                self.store_recs(json_split,arr)
                self.store_timestamp_event(json_split, arr)
                self.store_type_event(json_split, arr)
                event_csv.writerow(arr)
            elif(command == 'item_update'):
                arr = []
                self.store_item_update(json_split,arr)
                item_csv.writerow(arr)
            else:
                print('something went wrong')
                print(splitline)
            nrrows += 1
            if(nrrows % 100000 == 0):
                print(nrrows)
        return nrrows





p  = Processor()

nrrows = 0
for i in range(1, 28):
    event_csv = csv.writer(open('CSV/event_%s.csv' % i, 'w'), delimiter="\t")
    item_csv = csv.writer(open('CSV/item_update_%s.csv' % i, 'w'), delimiter="\t")
    if (i < 10):
        nrrows = p.parse('nr2016-02-0%s.log.gz' % i, 'rec_%s_csv.gz' % i, event_csv, item_csv, nrrows)
    else:
        nrrows = p.parse('nr2016-02-%s.log.gz' % i, 'rec_%s_csv.gz' % i, event_csv, item_csv, nrrows)

print(len(mapper.simple_keys))
print(len(mapper.simple_header))