#!/usr/bin/python
from gearman import GearmanWorker
# TODO: Initialize Logging
# TODO:  Direct log to file

# TODO: Gearman configuration file to be used
gm_worker = GearmanWorker(['localhost:4730'])
 
# TODO: Use Redis Configuration files
import redis
redis_server = redis.Redis('localhost')

#Other Libraries
import hashlib
import email
import re
import os

################################################
# Helper Functions
################################################
# TODO: can add more stop words
_STOP_WORDS = ['a', 'an','the', '']

# TODO: Can modify process_words to just process for inclusion list
# TODO: Stemming can be done
# TODO: Parse EMAIL content

# Taken from: https://bitquabit.com/post/having-fun-python-and-elasticsearch-part-2/
def unicodish(s):
    return s.decode('latin-1', errors='replace')

def parse_email(infile):
    fp = open(infile)
    message = email.message_from_file(fp)
    meta = {unicodish(k).lower(): unicodish(v) for k, v in message.items()}
    # TODO: Hackish way to split main email body from the reply thread
    # TODO: The split string might differ based on different email service providers
    # TODO: Make the split string config driven
    contents = unicodish(message.get_payload()).replace('\n', ' ').replace('\t', ' ').split('-----Original Message-----')
    body = contents[0]
    reply = contents[1:]
    fp.close()
    return body, reply, meta

def process_words(infile):
    body, reply, meta = parse_email(infile)
    words = re.split('\W+', body)
    cleaned_words = []
    for word in words:
        if word not in _STOP_WORDS:
        	cleaned_words.append(word.lower())
    #TODO: In addition to having the original file name, it can be hashed for more efficient storing at scale            
    return cleaned_words, reply, meta, infile, words[:10]

  
def inverted_index(cleaned_words, reply, meta, infile, sample_text):
    try:
        inverted = {}
        hash_object = hashlib.md5(infile)
        docid = hash_object.hexdigest()
        #NOTE: Prefetch the count of each word in a document
        counted_cleaned_words = dict((x,cleaned_words.count(x)) for x in set(cleaned_words))
        for word, count in counted_cleaned_words.iteritems():
            wordhash_object = hashlib.md5(word)
            wordhash = wordhash_object.hexdigest()
            word_doc_hash = wordhash + "_" + docid
            inverted[word] = {}
            inverted[word]['count'] = count
            for key, value in meta.iteritems():
                inverted[word][key] = value
            inverted[word]['sample_text'] = ' '.join(sample_text) + "..."
            #TODO: the base url can be served using a config
            inverted[word]['link'] = 'http://localhost:8000/'+'/'.join(os.path.dirname(infile).split('/')[-3:]) + '/' + os.path.basename(infile)
            inverted[word]['word'] = word
            inverted[word]['word_doc_hash'] = word_doc_hash
            redis_server.set(word_doc_hash+"New", str(inverted[word]))
            #TODO: For the sake of tracking in coming words, we can have a data store to persist each word flowing in
            #NOTE: tbp - to_be_processed list of words
            tbp_word = "tbp_" + word
            redis_server.set(tbp_word, "TBD")
        return "Ok"
    except Exception(e):
        #TODO: Log the error
        return str(e)

################################################

def task_listener_email(gearman_worker, gearman_job):
    #TODO: Log the call and params
    cleaned_words, reply, meta, infile, sample_text = process_words(gearman_job.data)
    #TODO: Log the call and params
    res = inverted_index(cleaned_words, reply, meta, infile, sample_text)
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
    gm_worker.set_client_id('inv-index-worker')
    gm_worker.register_task('invindex', task_listener_email)
    gm_worker.work()
