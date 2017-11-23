#!/usr/bin/python

import time, os, sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from gearman import GearmanClient
# TODO: Initialize Logging
# TODO:  Direct log to file

# TODO: Gearman configuration file to be used
relay = GearmanClient(['localhost:4730'])

class MyHandler(FileSystemEventHandler):
    '''
        Extending the base FileSystemEventHandler to relay to Gearman workers
    '''
    def on_created(self, event):
        print event.src_path, event.is_directory, event.event_type
        # TODO: Write above to log file
        if not (event.is_directory):
            # TODO: Write relayed to log file
            result = relay.submit_job('invindex', event.src_path,  background=True, wait_until_complete=False)
            # TODO: Write completed to log file

if __name__ == "__main__":
    # TODO:  Daemonize 
    # TODO: Ensure there are not multiple monitors for the same location (check for lock file)
    event_handler = MyHandler() 
    observer = Observer()
    observer.schedule(event_handler, path=sys.argv[1], recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(60) #-> make it config drive
    except SystemExit as e:
        observer.stop()
    observer.join()
