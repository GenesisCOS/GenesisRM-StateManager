import time 
import os 
import pathlib 
import json

import locust 
from locust import HttpUser, LoadTestShape
from locust import task, between, events


locust.stats.CONSOLE_STATS_INTERVAL_SEC = 600
locust.stats.HISTORY_STATS_INTERVAL_SEC = 60
locust.stats.CSV_STATS_INTERVAL_SEC = 60
locust.stats.CSV_STATS_FLUSH_INTERVAL_SEC = 60
locust.stats.CURRENT_RESPONSE_TIME_PERCENTILE_WINDOW = 60
locust.stats.PERCENTILES_TO_REPORT = [0.50, 0.80, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0]


HOST = os.getenv('HOST')
DATA_DIR = os.getenv('DATA_DIR')

RPS = [10, 10, 10, 10, 10]


request_log_file = open(pathlib.Path(DATA_DIR)/f'request.log', 'a')


class QuickStartUser(HttpUser):
    host = HOST 
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first = True 
        
    @events.request.add_listener
    def on_request(response_time, context, **kwargs):
        request_log_file.write(json.dumps({
            'time': time.perf_counter(),
            'latency': response_time / 1e3,
            'context': context,
        }) + '\n')
    
    def list_files(self):
        self.client.get('/')
        
    @task 
    def task(self):
        self.list_files()


class CustomShape(LoadTestShape):
    time_limit = len(RPS)
    spawn_rate = 100

    def tick(self):
        run_time = self.get_run_time()
        if run_time < self.time_limit:
            user_count = RPS[int(run_time)]
            return (user_count, self.spawn_rate)
        return None

