
import csv
from datetime import  datetime

from sys import argv

from recommenders.mpc_event_only import MPCEvent
from recommenders.mpc_views import MPCviews

try:
    BASEDIR = argv[1] # scratch-striped]
    cycle_time = argv[2]
    # session_only = argv[2] # wether or not to only recommend session > 1
    maxdays = 28
    mindays = 1
except:
    BASEDIR = '../Dataprocessing/CSV2/'
    session_only = True
    maxdays = 12
    mindays = 11
    cycle_time = 48

s = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
rfile = open('results/cycle_%s_results_%s.csv' % ( cycle_time,s), 'wt')
results_csv = csv.writer(rfile)

from recommenders.cooccur_based_ranker import CooccurRank
from recommenders.popular_based_recommender import PopRank
from recommenders.sequence_based_recommender import SeqRank
from recommenders.content_based_recommender import ContentRank
from recommenders.cooc_session_recommender import CoocSessionRank
from recommenders.mpc_session_recommender import SessionSeqRank
from recommenders.mpc_session_event_only import MPCEventSession
from recommenders.keyword_recommender import KeywordRecommender
from recommenders.hybrid_rec import HybridRec
from recommenders.poprank_event import PopRankEvent
from recommenders.epsilon_greedy import EpsilonGreedyPopEvent
from recommenders.high_risers_hybrid import HighRisersHybrid

coocrank = CooccurRank(BASEDIR)
poprank = PopRank(BASEDIR)
popevent = PopRankEvent(BASEDIR)
seqrank = SeqRank(BASEDIR)
contentrank = ContentRank(BASEDIR)
coocsessionrank = CoocSessionRank(BASEDIR)
mpcsessionrank = SessionSeqRank(BASEDIR)
mpceventsessionrank = MPCEventSession(BASEDIR)
mpcevent = MPCEvent(BASEDIR,cycle_time=cycle_time)
mpcviews = MPCviews(BASEDIR)
keywordrec = KeywordRecommender(BASEDIR)
most_popular_topic_rec = HybridRec(BASEDIR)
eps_greedy = EpsilonGreedyPopEvent(BASEDIR, flushing=True, flush_cycle=1)
high_riser_hybrid = HighRisersHybrid(BASEDIR, flushing=True, flush_cycle=True)


popevent_1 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=1)
popevent_2 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=2)
popevent_4 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=4)
popevent_8 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=8)
popevent_16 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=16)
popevent_24 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=24)
popevent_32 = PopRankEvent(BASEDIR, flushing=True, flush_cycle=32)




# rankers = [coocrank, poprank, seqrank, contentrank]
rankers = [coocrank,eps_greedy]



results = {'coocrank': {'day': {}}, 'poprank': {}, 'seqrank':{}, 'contentrank': {}}


def writeresults(day, ranker):
    print(ranker.name)
    print(ranker.evaluation.stats_site)
    print(ranker.evaluation.total_count_site)
    for key in ranker.evaluation.stats_site.keys():
        try:
            print('%s %s %s %s %s %s %s %s %s %s %s' %
                  (ranker.name,day, key, ranker.evaluation.stats_site[key], ranker.evaluation.total_count_site[key], ranker.evaluation.recall_site[key], len(ranker.evaluation.total_recall_site[key]),
                   ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key],
                   len(ranker.evaluation.recall_site[key])/len(ranker.evaluation.total_recall_site[key]),
                   ranker.evaluation.CG(key), ranker.evaluation.avgCG(key)))
            results_csv.writerow([ranker.name,day, key, ranker.evaluation.stats_site[key],
                                  ranker.evaluation.total_count_site[key],
                                  ranker.evaluation.stats_site[key]/ranker.evaluation.total_count_site[key],
                                  len(ranker.evaluation.recall_site[key])/len(ranker.evaluation.total_recall_site[key]),
                                  ranker.evaluation.CG(key), ranker.evaluation.avgCG(key)])
        except ZeroDivisionError:
            print('devision by zero')
            results_csv.writerow([ranker.name, day, key, ranker.evaluation.stats_site[key],  ranker.evaluation.total_count_site[key], 0,0, 0, 0])

for ranker in rankers:
    # ranker.run_test()
    # writeresults(1, ranker)
    for day in range (mindays,maxdays):
        ranker.init_new_day()
        ranker.run_day(day=day)
        writeresults(day, ranker)
        rfile.flush()





