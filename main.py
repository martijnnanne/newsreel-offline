
import csv
from datetime import  datetime

from sys import argv

from recommenders.mpc_event_only import MPCEvent

try:
    BASEDIR = argv[1] # scratch-striped]
    # session_only = argv[2] # wether or not to only recommend session > 1
    maxdays = 28
    mindays = 1
except:
    BASEDIR = '../Dataprocessing/CSV2/'
    session_only = True
    maxdays = 10
    mindays = 1

s = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
rfile = open('results/results_%s.csv' % s, 'wt')
results_csv = csv.writer(rfile)

from recommenders.cooccur_based_ranker import CooccurRank
from recommenders.popular_based_recommender import PopRank
from recommenders.sequence_based_recommender import SeqRank
from recommenders.content_based_recommender import ContentRank
from recommenders.cooc_session_recommender import CoocSessionRank
from recommenders.mpc_session_recommender import SessionSeqRank
from recommenders.mpc_session_event_only import MPCEventSession

coocrank = CooccurRank(BASEDIR)
poprank = PopRank(BASEDIR)
seqrank = SeqRank(BASEDIR)
contentrank = ContentRank(BASEDIR)
coocsessionrank = CoocSessionRank(BASEDIR)
mpcsessionrank = SessionSeqRank(BASEDIR)
mpceventsessionrank = MPCEventSession(BASEDIR)
mpcevent = MPCEvent(BASEDIR)

# rankers = [coocrank, poprank, seqrank, contentrank]


rankers = [mpcevent, mpceventsessionrank,coocrank, poprank,coocsessionrank,seqrank,contentrank,poprank]



results = {'coocrank': {'day': {}}, 'poprank': {}, 'seqrank':{}, 'contentrank': {}}


def writeresults(day, ranker):
    print(ranker.name)
    print(ranker.evaluation.stats_site)
    print(ranker.evaluation.total_count_site)
    for key in ranker.evaluation.stats_site.keys():
        try:
            print('%s %s %s %s %s %s %s %s %s' %
                  (ranker.name,day, key, ranker.evaluation.stats_site[key], ranker.evaluation.total_count_site[key], ranker.evaluation.recall_site[key], ranker.evaluation.total_recall_site[key],
                   ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key],
                   len(ranker.evaluation.recall_site[key])/len(ranker.evaluation.total_recall_site[key])))
            results_csv.writerow([ranker.name,day, key, ranker.evaluation.stats_site[key],
                                  ranker.evaluation.total_count_site[key],
                                  ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key],
                                  len(ranker.evaluation.recall_site[key])/len(ranker.evaluation.total_recall_site[key])])
        except ZeroDivisionError:
            print('devision by zero')
            results_csv.writerow([ranker.name, day, key, ranker.evaluation.stats_site[key],  ranker.evaluation.total_count_site[key], 0,0])

for ranker in rankers:
    # ranker.run_test()
    # writeresults(1, ranker)
    for day in range (mindays,maxdays):
        ranker.init_new_day()
        ranker.run_day(day=day)
        writeresults(day, ranker)
        rfile.flush()





