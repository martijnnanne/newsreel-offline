
import csv
from datetime import  datetime

from sys import argv
try:
    BASEDIR = argv[1] # scratch-striped]
    maxdays = 28
    mindays = 1
except:
    BASEDIR = '../Dataprocessing/'
    maxdays = 10
    mindays = 1

s = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
rfile = open('results/results_%s.csv' % s, 'wt')
results_csv = csv.writer(rfile)

from cooccur_based_ranker import CooccurRank
from popular_based_recommender import PopRank
from sequence_based_recommender import SeqRank
from content_based_recommender import ContentRank
from cooc_session_recommender import CoocSessionRank
from mpc_session_recommender import SessionSeqRank

coocrank = CooccurRank(BASEDIR)
poprank = PopRank(BASEDIR)
seqrank = SeqRank(BASEDIR)
contentrank = ContentRank(BASEDIR)
coocsessionrank = CoocSessionRank(BASEDIR)
mpcsessionrank = SessionSeqRank(BASEDIR)


# rankers = [coocrank, poprank, seqrank, contentrank]
rankers = [ mpcsessionrank,coocsessionrank,contentrank,seqrank,poprank,coocrank]

results = {'coocrank': {'day': {}}, 'poprank': {}, 'seqrank':{}, 'contentrank': {}}


def writeresults(day, ranker):
    print(ranker.name)
    print(ranker.evaluation.stats_site)
    print(ranker.evaluation.total_count_site)
    for key in ranker.evaluation.stats_site.keys():
        try:
            print('%s %s %s %s %s %s' %
                  (ranker.name,day, key, ranker.evaluation.stats_site[key], ranker.evaluation.total_count_site[key],  ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key]))
            results_csv.writerow([ranker.name,day, key, ranker.evaluation.stats_site[key],
                                 ranker.evaluation.total_count_site[key],  ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key]])
        except ZeroDivisionError:
            print('devision by zero')
            results_csv.writerow([ranker.name, day, key, ranker.evaluation.stats_site[key], ranker.evaluation.total_count_site[key], 0])

for ranker in rankers:
    # ranker.run_test()
    for day in range (mindays,maxdays):
        ranker.init_new_day()
        ranker.run_day(day=day)
        writeresults(day, ranker)
        rfile.flush()





