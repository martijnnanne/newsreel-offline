
import csv
import json
from datapreprocessing.mapping_preprocessing import Mapping
import gzip

mapper = Mapping()

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("event_dir", help="directory to put parsed event files")
parser.add_argument("rec_dir", help="directory to put parsed rec files")
parser.add_argument("item_dir", help="directory to put parsed item update files")
parser.add_argument("bridge_dir", help="directory of item ordering (used in the offline recommender systems in order to replay the data)")
parser.add_argument("infile_dir", help="directory of the raw input files")

args = parser.parse_args()
event_dir = args.event_dir
rec_dir = args.rec_dir
item_dir  = args.item_dir
bridge_dir = args.bridge_dir
infile_dir = args.infile_dir

class Processor():

    def __init__(self):
        self.idx_event = 0
        self.idx_update = 0
        self.idx_rec = 0
        self.nrrows = 0
        self.event_mapping = mapper.get_header_event()
        self.user_item_dict = {}

    def reset_item_dict(self):
        self.user_item_dict = {}

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

    def store_limit(self, json_split, arr):
        try:
            for key in self.mapper.limit:
                try:
                    arr.append(json_split[key])
                except:
                    arr.append('')
        except:
            pass


    def parse(self, infile, outfile,event_csv, item_csv, bridge_csv, nrrows):
        file = gzip.open('%s/%s' % (infile_dir,infile), 'rt')
        rec_csv = csv.writer(gzip.open('%s/%s' % (rec_dir,outfile), 'wt'), delimiter="\t")

        rec_csv.writerow(mapper.simple_header + mapper.cluster_header + mapper.list_header)
        item_csv.writerow(mapper.item_update_keys)
        event_csv.writerow(mapper.simple_header + mapper.cluster_header + mapper.list_header + mapper.recs + mapper.timestamp + mapper.type)
        i = 0
        event_mapping = mapper.get_header_event()
        user_id = 1
        for line in file:
            splitline = line.split('\t')
            command = splitline[0]
            json_split = self.getJson(splitline[1])
            if(command == 'recommendation_request'):
                arr = []
                self.store_simple_context(json_split, arr)
                self.store_cluster_context(json_split, arr)
                self.store_list_context(json_split, arr)
                self.store_limit(json_split, arr)
                rec_csv.writerow(arr)
                bridge_csv.writerow(['rec', self.idx_rec])
                self.idx_rec +=1
            elif(command == 'event_notification'):
                self.store_event(json_split, event_csv, bridge_csv)
            elif(command == 'item_update'):
                arr = []
                self.store_item_update(json_split,arr)
                item_csv.writerow(arr)
                bridge_csv.writerow(['update', self.idx_update])
                self.idx_update +=1
            else:
                print('something went wrong')
                print(splitline)
            nrrows += 1
            if(nrrows % 100000 == 0):
                print("processed", nrrows, "rows")
        return nrrows

    def store_event(self, json_split, event_csv, bridge_csv):
        arr = []
        self.store_simple_context(json_split, arr)
        self.store_cluster_context(json_split, arr)
        self.store_list_context(json_split, arr)
        self.store_recs(json_split, arr)
        self.store_timestamp_event(json_split, arr)
        self.store_type_event(json_split, arr)
        event_csv.writerow(arr)
        bridge_csv.writerow(['event', self.idx_event])
        self.idx_event += 1




p  = Processor()

nrrows = 0
for i in range(1,28):
    p.reset_item_dict()
    event_csv = csv.writer(open('%s/event_%s.csv' % (event_dir, i), 'w'), delimiter="\t")
    item_csv = csv.writer(open('%s/item_update_%s.csv' % (item_dir, i), 'w'), delimiter="\t")
    file = gzip.open('%s/bridge_table_%s_csv.gz' % (bridge_dir, i), 'wt')
    bridge_csv = csv.writer(file, delimiter="\t")
    bridge_csv.writerow(['type', 'idx'])
    if (i < 10):
        nrrows = p.parse('nr2016-02-0%s.log.gz' % i, 'rec_%s_csv.gz' % i, event_csv, item_csv, bridge_csv, nrrows)
    else:
        nrrows = p.parse('nr2016-02-%s.log.gz' % i, 'rec_%s_csv.gz' % i, event_csv, item_csv,bridge_csv, nrrows)

