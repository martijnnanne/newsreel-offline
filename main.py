
import csv
from datetime import  datetime

from sys import argv

from recommenders.mpc_event_only import MPCEvent
from recommenders.mpc_views import MPCviews

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("BASEDIR", help="Directory with the parsed data directories in it")
parser.add_argument("session_only", help="Evaluate only on session clicks", default=False)
args = parser.parse_args()
BASEDIR = args.BASEDIR
session_only = args.session_only
# try:
#     BASEDIR = argv[1] # scratch-striped]
#     cycle_time = argv[2]
#     # session_only = argv[2] # wether or not to only recommend session > 1
#     maxdays = 28
#     mindays = 1
# except:
#     BASEDIR = '../Dataprocessing/CSV2/'
#     session_only = True
#     maxdays = 3
#     mindays = 1
#     cycle_time = 48

s = datetime.now().strftime('%Y-%m-%d:%H:%M:%S')
rfile = open('results/cycle_%s_results_%s.csv', 'wt')
results_csv = csv.writer(rfile)

from recommenders.cooccur_based_ranker import CooccurRank
from recommenders.popular_based_recommender import PopRank, PopRankViews
from recommenders.sequence_based_recommender import SeqRank
from recommenders.content_based_recommender import ContentRank
from recommenders.cooc_session_recommender import CoocSessionRank
from recommenders.mpc_session_recommender import SessionSeqRank
from recommenders.mpc_session_event_only import MPCEventSession
from recommenders.keyword_recommender import KeywordRecommender
from recommenders.most_popular_topic import MostPopularTopic
from recommenders.most_clicked import MostClicked
from recommenders.epsilon_greedy import EpsilonGreedyPopEvent
from recommenders.explore_hybrid import *

coocrank = CooccurRank(BASEDIR, session_only=session_only)
poprec = PopRank(BASEDIR,session_only=session_only)
poprankviews = PopRankViews(BASEDIR,session_only=session_only)
most_clicked = MostClicked(BASEDIR, session_only=session_only)
mpseq = SeqRank(BASEDIR,session_only=session_only)
stemrec = ContentRank(BASEDIR)
coocsessionrank = CoocSessionRank(BASEDIR,session_only=session_only)
mpseqsessionrank = SessionSeqRank(BASEDIR,session_only=session_only)
mpseqeventsessionrank = MPCEventSession(BASEDIR,session_only=session_only)
mpseqevent = MPCEvent(BASEDIR,session_only=session_only)
mpseqviews = MPCviews(BASEDIR,session_only=session_only)
keywordrec = KeywordRecommender(BASEDIR,session_only=session_only)
most_popular_topic_rec = MostPopularTopic(BASEDIR,session_only=session_only)
eps_greedy = EpsilonGreedyPopEvent(BASEDIR, flushing=True, flush_cycle=1,session_only=session_only)

popevent_mpseqall_hybrid = PopMpcAll(BASEDIR)
popevent_key_hybrid = PopKeyHybrid(BASEDIR)
popevent_pop_hybrid = PopPophybrid(BASEDIR)

mpseqevent_clicked_only = MPCSOLO(BASEDIR)
poprec_clicked_only = POPSOLO(BASEDIR)

# poprank with different flush cycles (when a flush cycle is called, only the top 250 most popular itmems are retained)
popevent_1 = MostClicked(BASEDIR, flushing=True, flush_cycle=1, session_only=session_only)
popevent_2 = MostClicked(BASEDIR, flushing=True, flush_cycle=2, session_only=session_only)
popevent_4 = MostClicked(BASEDIR, flushing=True, flush_cycle=4, session_only=session_only)
popevent_8 = MostClicked(BASEDIR, flushing=True, flush_cycle=8, session_only=session_only)
popevent_16 = MostClicked(BASEDIR, flushing=True, flush_cycle=16, session_only=session_only)
popevent_24 = MostClicked(BASEDIR, flushing=True, flush_cycle=24, session_only=session_only)
popevent_32 = MostClicked(BASEDIR, flushing=True, flush_cycle=32, session_only=session_only)

mpceventsessionrank_1 = MPCEventSession(BASEDIR, 1,session_only=session_only)
mpceventsessionrank_2 = MPCEventSession(BASEDIR, 0.8,session_only=session_only)
mpceventsessionrank_3 = MPCEventSession(BASEDIR, 0.6,session_only=session_only)
mpceventsessionrank_4 = MPCEventSession(BASEDIR, 0.5,session_only=session_only)
mpceventsessionrank_5 = MPCEventSession(BASEDIR, 0.3,session_only=session_only)
mpceventsessionrank_6 = MPCEventSession(BASEDIR, 0.2,session_only=session_only)
mpceventsessionrank_7 = MPCEventSession(BASEDIR, 0.1,session_only=session_only)
mpceventsessionrank_8 = MPCEventSession(BASEDIR, 0.0,session_only=session_only)


##INSERT RANKERS HERE##

#experiment 1
rankers = [coocrank,poprec,most_clicked,mpseq,stemrec,mpseqevent,keywordrec,most_popular_topic_rec]

#experiment 2
#rankers = [mpseqsessionrank, mpseq, mpseqeventsessionrank, mpseqevent, coocrank, coocsessionrank]

#etc..



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

# iterate over each ranker
for ranker in rankers:
    # iterate over each day
    for day in range (1,28):
        ranker.init_new_day()
        ranker.run_day(day=day)
        writeresults(day, ranker) # store all results in the result file which can be later imported in fancy_graphs.ipynb
        rfile.flush()





