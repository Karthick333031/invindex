#!/usr/bin/python
# TODO: Initialize Logging
# TODO:  Direct log to file 
# TODO: Use Gearman Configuration files
from gearman import GearmanClient
topnrelay = GearmanClient(['localhost:4730'])

# TODO: Use Redis Configuration files
import redis
redis_server = redis.Redis('localhost')


if __name__ == "__main__":
	# TODO: We can have multiple clients by allowing for an extended pattern say "tbp_a*" - <all words to be processed starting with a>
	# TODO: This can be config driven based on the scale that has to be achieved
	tbp_keys = redis_server.keys(pattern='tbp_*')
	for word in tbp_keys:
		# TODO: Most frequent words can be sent into the top N compute queue with higher priority
		# TODO: Log the call        
		result = topnrelay.submit_job('topncompute', word,  background=True, wait_until_complete=False)
