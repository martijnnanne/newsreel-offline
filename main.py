
import csv
from datetime import  datetime

from sys import argv
try:
    BASEDIR = argv[1] # scratch-striped
except:
    BASEDIR = ''

s = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
results_csv = csv.writer(open('results/results_%s.csv' % s, 'wt'))

from cooccur_based_ranker import CooccurRank
from popular_based_recommender import PopRank
from sequence_based_recommender import SeqRank
from content_based_recommender import ContentRank

coocrank = CooccurRank(BASEDIR)
poprank = PopRank(BASEDIR)
seqrank = SeqRank(BASEDIR)
contentrank = ContentRank(BASEDIR)


# rankers = [coocrank, poprank, seqrank, contentrank]
rankers = [contentrank,coocrank, poprank, seqrank]

results = {'coocrank': {'day': {}}, 'poprank': {}, 'seqrank':{}, 'contentrank': {}}


def writeresults(day, ranker):
    print(ranker.name)
    print(ranker.evaluation.stats_site)
    print(ranker.evaluation.total_count_site)
    for key in ranker.evaluation.stats_site.keys():
        try:
            print('%s %s %s %s' %(ranker.name,day, key, ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key]))
            results_csv.writerow([ranker.name, day, key, ranker.evaluation.stats_site[key] / ranker.evaluation.total_count_site[key]])
        except ZeroDivisionError:
            print('devision by zero')
            results_csv.writerow([ranker.name, day, key, 0])

for ranker in rankers:
    ranker.run_test()
    for day in range (1,4):
        ranker.init_new_day()
        ranker.run_day(day=day)
        writeresults(day, ranker)





