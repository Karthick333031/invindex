### Inverted Index 

## Requirements

> Download & Build redis 
> brew install gearman
> ln -s /usr/local/opt/gearman/sbin/gearmand /usr/local/bin
> pip install gearman
> pip install redis
> pip install tornado

> Check import heapq works?
> Check import hashlib works?

Start Gearman: gearmand -d
Start Redis Server

## Module details:

mail_watcher.py
  Usage: python mail_watcher.py <full path in local file system to be monitored>
  This serves as the gearman client to post the word to a "invindex" queue

invindex_processor.py
  Usage: python invindex_processor.py 
  This is unit task worker and listens to messages in invindex queue in gearman

topN_word_distributor.py
  Usage: python topN_word_distributor.py
  This is subsequent gearman client that can be run at any interval
  This is required to poll for the newly created/parsed words from new files
  On identifying the bunch of new words, this posts respective message to "topncompute" queue

topN_word_processor.py
  Usage: python topN_word_processor.py
  This is a task worker that listens to "topncompute" queue
  The primary task is to build an easily retrievable TopN documents for every single word

topN_serve.py
  Usage: python topN_serve.py
  Async Tornado server to cater to user requests
  Parses the user requests and applies the necessary filtering to either provide "AND" or "OR" functionalities
  Results can be obtained by browsing to 
    - http://localhost:8888/query?query=<AND word1 word2>
    - http://localhost:8888/query?query=<OR word1 word2>

httpd.py
  Usage: python httpd.py
  This starts a http server a local folder and serves static content
  This is to be used for being able to access the file link   

folder: qa
This contains the sample files against which I have done my testing

## Known Limitations:
> As much as possible I have commented potential enhancements as TODO & Potential key points as NOTE
> Primarily relied on python & pythonic implementations (Example: Directly used a heapq package instead of writing heap search algorithm)    
> I do not have expertise in building UI, Hence the output of the request throws out a JSON