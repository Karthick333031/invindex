#!/usr/bin/python
from gearman import GearmanWorker
# TODO: Initialize Logging

# TODO: Gearman configuration file to be used
gm_worker = GearmanWorker(['localhost:4730'])

#Data Structure to go for storing/retrieving top N results 
import heapq

import hashlib

# TODO: Use Redis Configuration files
import redis
redis_server = redis.Redis('localhost')



########################################################################
#  Helper functions
########################################################################

def topncompute(word):
    #NOTE: Heap List on which top N will be computed
    all_topn_compare = []

    #NOTE: Dict on which data for a word from new/old files came in    
    topncompute = {}

    #NOTE: The string to be replaced can be customized based on the pattern that you want
    hash_object = hashlib.md5(word.replace('tbp_', ''))
    wordhash = hash_object.hexdigest()
    wordhash_pattern = wordhash + "_*New"
    topncompute_keys = redis_server.keys(pattern=wordhash_pattern)
    topncompute[wordhash] = {}
    # NOTE: "New" is added to the hash pattern to process only the newly incoming data
    for key in topncompute_keys:
        word_doc_hash = None
        word_doc_hash = key.replace('New','').split('_')
        #NOTE: wordhash and word_doc_hash[0] has to be same        
        topncompute[word_doc_hash[0]][word_doc_hash[1]] = eval(redis_server.get(key))
        redis_server.set(word_doc_hash[0]+"_"+word_doc_hash[1], topncompute[word_doc_hash[0]][word_doc_hash[1]])
        redis_server.delete(key)

    
	# NOTE: Get the already present top N from 
	current_topn_pattern = wordhash + "TopN"

    # NOTE: The current Top N returned has to be eval(dict) for it to be processed
    # NOTE: Sample <wordhash>_topn: {1: {doc1 with properties}, 2: {doc2 with properties}}
    current_topn  = redis_server.get(current_topn_pattern)
    
    current_topn_compare = []
    if current_topn is not None:
        # NOTE: The persisted top N would be a list of dictionaries
        current_topn_compare = eval(current_topn)    
        for each_item in current_topn_compare:                    
            all_topn_compare.extend(each_item.values())

    for k, v in topncompute[word_doc_hash[0]].iteritems():
        all_topn_compare.append(v)
    
    # NOTE: Get top N by count
    # NOTE: This solution below gives out of the box sorting on the heap q by descending order
    # NOTE: If the ordering has to be done by one or many, a more sophisticated sorting has to be done 
    # NOTE: We can offload that burden to the client after responding with all possible filters to sort on
    
    N = len(all_topn_compare)

    # NOTE: N can be capped to avoid a huge dictionary as a value

    topN = heapq.nlargest(N, all_topn_compare, key=lambda s: s['count'])
    topN_list = [{item['word_doc_hash'].split('_')[1]:item} for item in topN]
    
    # NOTE: Persist topN into redis
    redis_server.set(current_topn_pattern, str(topN_list))

    # NOTE: remove the tbp_<word> from the to be processed list
    redis_server.delete(word) 
    return 'Ok'   

########################################################################


def task_listener_topncompute(gearman_worker, gearman_job):
    #TODO: Log the call and params
    res = topncompute(gearman_job.data)
    print res
    #TODO: Log the result
    if res != 'Ok':
        # Log the stack trace and exception
        return 'NotOk'
    else:
        return res

if __name__ == "__main__":
    # TODO:  Daemonize 
    # TODO: Ensure there are not multiple monitors for the same location (check for lock file)
    # TODO: Make the strings config driven
    # TODO: Have a meaningful try-except block
    gm_worker.set_client_id('topn-worker')
    gm_worker.register_task('topncompute', task_listener_topncompute)
    gm_worker.work()