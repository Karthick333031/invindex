import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.gen as gen

import heapq
import hashlib

# TODO: Use Redis Configuration files
import redis
redis_server = redis.Redis('localhost')

# TODO: Initialize logging

class topN(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @gen.engine
    def get(self):
        try:
            query = self.get_argument('query')
            print query
            # NOTE: Async call
            response = yield gen.Task(self.process_query, query)
            self.write(response)
            self.finish()
        except Exception(e):
            print str(e)


    def process_query(self, query, callback):
        try:
            # NOTE: First check if the query starts with and / or
    	    result = None
            docs = []
            query_list = query.split()
            if query_list[0].lower() not in ['and', 'or']:
                result = "Not a valid Pattern. Please query it as 'AND' followed by word(s) or 'OR' followed by word(s)" 
            if query_list[0].lower() == 'and':
                # NOTE: Get the TopN for the first word to form the base set
                # NOTE: Get the TopN for subsequent words and keep doing intersection with the first word base set                
                # NOTE: Get the total count of all the words by document
                # NOTE: Reorder the document sequence sorted by the total counts of all the input words across documents
                # NOTE: Build a unique list of documents in sorted order
                # NOTE: For this unique list of documents in that order obtain all the necessary response string
                first_word_hash = hashlib.md5(query_list[1]).hexdigest()
                first_word_topn_list = eval(redis_server.get(first_word_hash+"TopN"))
                first_word_docs = []
                for item in first_word_topn_list:
                    first_word_docs.extend(item.keys())
                    docs.append(item)

                total_set = set(first_word_docs)
                for word in query_list[2:]: 
                    next_word_docs = []
                    wordhash = hashlib.md5(word).hexdigest()
                    next_word_topn_list = eval(redis_server.get(wordhash+"TopN"))
                    for item in next_word_topn_list:
                        next_word_docs.extend(item.keys())
                        docs.append(item)
                    total_set = total_set & (set(next_word_docs))
                    
                total_set_list = list(total_set)
                
                reorder_list = []
                if total_set:
                    for each_doc in total_set_list:
                        entry = {}
                        # NOTE: Filter only the necessary documents associated with each words
                        filtered_list = filter(lambda x: x.keys()[0]==each_doc, docs)
                        entry['doc'] = each_doc
                        entry['count'] = 0
                        for item in filtered_list:
                            entry['count'] = entry['count'] + int(item.values()[0]['count'])
                            reorder_list.append(entry)

                    N = len(reorder_list)
                    reordered_topN = heapq.nlargest(N, reorder_list, key=lambda s: s['count'])

                    final_docs = []
                    for i in reordered_topN:
                        if i['doc'] not in final_docs:
                            final_docs.append(i['doc'])

                    final_topN=[]
                    for each_doc in final_docs:
                        final_topN.extend(filter(lambda x: x.keys()[0]==each_doc, docs))
                    result = str(final_topN)
                else:
                    # NOTE: If the final intersection of sets is empty
                    result = "Not a single doc with all these words"    

            elif query_list[0].lower() == 'or':
                # NOTE: Get the TopN for the first word to form the base set
                # NOTE: Get the TopN for subsequent words and keep doing intersection with the first word base set                
                # NOTE: Get the total count of all the words by document

                first_word_hash = hashlib.md5(query_list[1]).hexdigest()
                first_word_topn_list = eval(redis_server.get(first_word_hash+"TopN"))
                first_word_docs = []
                for item in first_word_topn_list:
                    first_word_docs.extend(item.keys())
                    docs.append(item)

                total_set = set(first_word_docs)
                for word in query_list[2:]: 
                    next_word_docs = []
                    wordhash = hashlib.md5(word).hexdigest()
                    next_word_topn_list = eval(redis_server.get(wordhash+"TopN"))
                    for item in next_word_topn_list:
                        next_word_docs.extend(item.keys())
                        docs.append(item)
                    total_set = total_set | (set(next_word_docs))
                    
                total_set_list = list(total_set)
                filtered_list=[]
                if total_set:
                    for each_doc in total_set_list:
                        filtered_list.extend(filter(lambda x: x.keys()[0]==each_doc, docs))

                reordered_topN = []
                for each_doc in filtered_list:
                    reordered_topN.extend(each_doc.values())

                N = len(reordered_topN)    

                topN = heapq.nlargest(N, reordered_topN, key=lambda s: s['count'])
                final_topN = [{item['word_doc_hash'].split('_')[1]:item} for item in topN]

                result = str(final_topN)                
            else:
                result = "Not a valid Pattern. Please query it as 'AND' followed by word(s) or 'OR' followed by word(s)" 
                
        except Exception(e):
            print str(e)

        return callback(result)

application = tornado.web.Application([
    (r"/query", topN),
])

http_server = tornado.httpserver.HTTPServer(application)
http_server.listen(8888)
tornado.ioloop.IOLoop.instance().start()