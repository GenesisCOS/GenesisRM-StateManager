import logging as log
import time
import fcntl 
import random
import os 
import json 
import threading 
from pathlib import Path

import numpy as np
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor
)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from requests import utils as requests_utils
from locust import HttpUser, LoadTestShape, task, between, events
from locust.log import setup_logging
import locust.stats

WAIT_MIN = 2
WAIT_MAX = 5 

locust.stats.CONSOLE_STATS_INTERVAL_SEC = 600
locust.stats.HISTORY_STATS_INTERVAL_SEC = 60
locust.stats.CSV_STATS_INTERVAL_SEC = 60
locust.stats.CSV_STATS_FLUSH_INTERVAL_SEC = 50
locust.stats.CURRENT_RESPONSE_TIME_PERCENTILE_WINDOW = 60
locust.stats.PERCENTILES_TO_REPORT = [0.50, 0.80, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0]

setup_logging("INFO", None)

DATA_DIR = os.getenv('DATA_DIR')

LOCUST_INDEX = int(os.getenv('LOCUST_INDEX'))

request_log_file = None 
    
WAIT_TIME = os.getenv('WAIT_TIME')
if WAIT_TIME is not None:
    WAIT_MIN = float(WAIT_TIME)
    WAIT_MAX = int(WAIT_TIME)

DATASET = os.getenv('DATASET')
SERVICE_NAME = os.getenv('SERVICE_NAME')

CLUSTER_GROUP = os.getenv('CLUSTER_GROUP')

CLUSTER = 'production'

if DATASET is not None:
    RPS = list(map(int, Path(DATASET).read_text().splitlines()))
else:
    RPS = [10] * 5

def possible(x):
    """ x = 0 ~ 1 """
    return True if random.random() < x else False


WAIT_TIME = 10
PLUS = 25
ADD = 5
OFFSET = 210

""" Init opentelemetry """

resource = Resource(attributes={
    "service.name": SERVICE_NAME,
    "service.version": "0.0.1",
    "cluster.group": CLUSTER_GROUP,
})

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(
    endpoint="http://127.0.0.1:4317", 
    insecure=True
))
provider.add_span_processor(processor)

# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("my.tracer.name")

""" Init opentelemetry end """

REAL_START_TIME = None 
RESPONSE_TIMES = list()
REPORT_INTERVAL = 60 

STATS_LOCK = threading.Lock()

CLIENT_ID = None 

REQUEST_TIMEOUT = (0.1, 3)

class QuickStartUser(HttpUser):
    wait_time = between(WAIT_MIN, WAIT_MAX)

    VERIFY_CODE_URL = "/api/v1/verifycode/generate"
    LOGIN_URL = "/api/v1/users/login"
    TRAIN_SERVICE_URL = "/api/v1/trainservice"
    STATION_SERVICE_URL = "/api/v1/stationservice"
    TRAVEL_PLAN_SERVICE_URL = "/api/v1/travelplanservice"
    ORDER_SERVICE = "/api/v1/orderservice"
    ORDER_OTHER_SERVICE = "/api/v1/orderOtherService"
    FOOD_SERVICE = "/api/v1/foodservice"
    ASSURANCE_SERVICE = "/api/v1/assuranceservice"
    CONTACTS_SERVICE = "/api/v1/contactservice"
    PRESERVE_SERVICE = "/api/v1/preserveservice"

    TRAVEL_SEARCH_TARGET = ["cheapest", "quickest"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first = True 
        self.user_uuid = 'user-uuid-xxxxxxxxxxx'
        self.user_id = dict()
        self.user_token = dict()
        self.synced = False 
        self.trains = list()
        self.stations = list()
        self.start_station = None
        self.end_station = None
        self.get_stations = False
        
    @events.report_to_master.add_listener
    def on_report_to_master(client_id, data):
        global CLIENT_ID
        CLIENT_ID = client_id
        
    @events.request.add_listener
    def on_request(response_time, context, name, exception, **kwargs):
        global RESPONSE_TIMES, REAL_START_TIME, REPORT_INTERVAL
        global request_log_file
        
        if not os.path.exists(DATA_DIR):
            return 
        
        time_ = time.time()
        
        if LOCUST_INDEX == 1:
            ok = STATS_LOCK.acquire(blocking=False)
            if ok:
                if REAL_START_TIME is None:
                    REAL_START_TIME = time_ 
                    with open(f'{DATA_DIR}/real_start_time.txt', 'w+') as real_start_time_file:
                        real_start_time_file.write(f'{REAL_START_TIME}')
                        real_start_time_file.flush()
                else:
                    if time_ - REAL_START_TIME > REPORT_INTERVAL:
                        real_time_start__ = REAL_START_TIME
                        
                        with open(f'{DATA_DIR}/request.log') as request_log:
                            for line in request_log.readlines():
                                try:
                                    request = json.loads(line)
                                    RESPONSE_TIMES.append(request['latency'])
                                except:
                                    continue
                        
                        with open(f'{DATA_DIR}/requests.csv', 'w+') as request_csv:
                            results = np.array(RESPONSE_TIMES)
                            
                            mean_rt = np.mean(results)
                            max_rt = np.max(results)
                            min_rt = np.min(results)
                            p99_rt = np.percentile(results, 99)
                            p95_rt = np.percentile(results, 95)
                            p90_rt = np.percentile(results, 90)
                            p50_rt = np.percentile(results, 50)
                            rps = len(results) / (time_ - real_time_start__)
                            
                            request_csv.write(
                                'mean_rt,max_rt,min_rt,p99_rt,p95_rt,p90_rt,p50_rt,rps\n'
                                f'{mean_rt},{max_rt},{min_rt},{p99_rt},{p95_rt},{p90_rt},{p50_rt},{rps}\n'
                            )
                            request_csv.flush()
                        
                        REAL_START_TIME = time.time() + 20
                        with open(f'{DATA_DIR}/real_start_time.txt', 'w+') as real_start_time_file:
                            real_start_time_file.write(f'{REAL_START_TIME}')
                            real_start_time_file.flush()
                        time.sleep(20)
                        
                        os.remove(f'{DATA_DIR}/request.log')
                        
                        RESPONSE_TIMES = list()
                STATS_LOCK.release()
            
        latency = response_time
        
        if LOCUST_INDEX != 1:
            while True:
                if os.path.exists(f'{DATA_DIR}/real_start_time.txt'):
                    break 
                time.sleep(1)
            with open(f'{DATA_DIR}/real_start_time.txt', 'r') as real_start_time_file:
                while True:
                    txt = real_start_time_file.read()
                    if txt == '':
                        continue
                    if REAL_START_TIME is not None and (float(txt) - REAL_START_TIME) < 0:
                        continue 
                    break 
                REAL_START_TIME = float(txt)
        
        if exception is None:
            STATS_LOCK.acquire()
            with open(f'{DATA_DIR}/request.log', 'a') as request_log_file:
                
                start_time = context['start_time_ms'] / 1000
                if start_time >= REAL_START_TIME:
                    fcntl.flock(request_log_file, fcntl.LOCK_EX)
                    request_log_file.write(json.dumps(dict(
                        time=time_,
                        latency=latency,
                        context=context,
                        name=name,
                        client_id=CLIENT_ID
                    )) + '\n')
                    request_log_file.flush()
                    fcntl.flock(request_log_file, fcntl.LOCK_UN)
                
            STATS_LOCK.release()
        else:
            STATS_LOCK.acquire()
            with open(f'{DATA_DIR}/error.log', 'a') as request_log_file:
                
                start_time = context['start_time_ms'] / 1000
                if start_time >= REAL_START_TIME:
                    fcntl.flock(request_log_file, fcntl.LOCK_EX)
                    request_log_file.write(json.dumps(dict(
                        time=time_,
                        context=context,
                        name=name,
                        client_id=CLIENT_ID,
                        exception=str(exception)
                    )) + '\n')
                    request_log_file.flush()
                    fcntl.flock(request_log_file, fcntl.LOCK_UN)
                
            STATS_LOCK.release()

    @property
    def local_time(self):
        return time.localtime(time.time())

    def on_start(self):
        pass
    
    def on_stop(self):
        pass

    def get_verify_code(self):
        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        headers = {
            "Content-Type": "application/json",
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        resp = self.client.get(QuickStartUser.VERIFY_CODE_URL, context=context, headers=headers, timeout=REQUEST_TIMEOUT)
        cookies = resp.cookies
        verify_code_cookies_dict = requests_utils.dict_from_cookiejar(cookies)
        verify_code_cookies_dict["answer"] = resp.headers.get("VerifyCodeAnswer")
        return verify_code_cookies_dict

    def login(self, username, password):
        verify_code_cookies_dict = self.get_verify_code()
        headers = {
            "Content-Type": "application/json",
            "Cookie": "YsbCaptcha=" + str(verify_code_cookies_dict["YsbCaptcha"]),
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        data = {
            "username": username,
            "password": password,
            "verificationCode": verify_code_cookies_dict["answer"]
        }

        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        resp = self.client.post(
            QuickStartUser.LOGIN_URL,
            json=data,
            headers=headers,
            context=context,
            timeout=REQUEST_TIMEOUT
        )

        if str(resp.headers.get("Content-Type")) == "application/json":
            resp_json_dict = resp.json()
            login_status = resp_json_dict["status"]
            msg = resp_json_dict["msg"]
            if login_status == 1 and msg == "login success":
                user_id = str(resp_json_dict["data"]["userId"])
                token = str(resp_json_dict["data"]["token"])
                self.user_id[username] = user_id
                self.user_token[username] = token
            else:
                pass
        else:
            pass

    def sync_info(self):
        if self.synced:
            return 
        
        """ get all trains """
        headers = {
            "Content-Type": "application/json",
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }
        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }
        resp = self.client.get(
            "/api/v1/trainservice/trains",
            context=context, headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        resp_json_dict = resp.json()
        if resp_json_dict["status"] == 1 and resp_json_dict["msg"] == "success":
            trains_list = resp_json_dict["data"]
            for train in trains_list:
                train_id = train["id"]
                train_average_speed = train["averageSpeed"]
                self.trains.append({"train_id": train_id, "train_ave_speed": train_average_speed})

        """ get all stations """
        headers["requestSendTimeNano"] = "{:.0f}".format(time.time() * 1000000000)
        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }
        resp = self.client.get(QuickStartUser.STATION_SERVICE_URL + "/stations",
                               context=context, headers=headers, timeout=REQUEST_TIMEOUT)
        resp_json_dict = resp.json()
        if resp_json_dict["status"] == 1 and resp_json_dict["msg"] == "Find all content":
            stations_list = resp_json_dict["data"]
            for station in stations_list:
                station_id = station["id"]
                station_name = station["name"]
                station_stay_time = station["stayTime"]
                
                self.stations.append(dict(
                    station_id=station_id,
                    station_name=station_name,
                    station_stay_time=station_stay_time
                ))
        
        self.start_station = None 
        self.end_station = None 
        
        for station in self.stations:
            if station['station_name'] == 'Shang Hai':
                self.start_station = station 
            if station['station_name'] == 'Su Zhou':
                self.end_station = station 
                
        assert self.start_station is not None 
        assert self.end_station is not None 
        
        self.synced = True 

    def search_travel_plan(self, target, starting_place, end_place, departure_time):
        
        if starting_place is None or end_place is None or departure_time is None:
            return None

        headers = {
            "Content-Type": "application/json",
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }
        data = {
            "startingPlace": starting_place,
            "endPlace": end_place,
            "departureTime": departure_time,  # java Date
        }

        if target != "cheapest" and target != "quickest" and target != "minStation":
            return

        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        url = QuickStartUser.TRAVEL_PLAN_SERVICE_URL + "/travelPlan/%s" % (target,)
        with tracer.start_as_current_span(f"locust@travel-plan-service@{url}") as span:
            resp = self.client.post(
                url=url,
                json=data,
                headers=headers,
                context=context,
                timeout=REQUEST_TIMEOUT
            )
        try:
            resp_json_dict = resp.json()
        except:
            resp_json_dict = None 
        if resp_json_dict is None:
            return None 
        if resp_json_dict["status"] == 1:
            return resp_json_dict["data"]
        else:
            return None

    def query_orders(self, login_id,
                     enable_travel_date_query: bool,
                     enable_bought_date_query: bool,
                     enable_state_query: bool,
                     **kwargs):

        orders = []
        order_others = []

        travel_date_start = kwargs["travel_date_start"] if "travel_date_start" in kwargs else None
        travel_date_end = kwargs["travel_date_end"] if "travel_date_end" in kwargs else None
        bought_date_start = kwargs["bought_date_start"] if "bought_date_start" in kwargs else None
        bought_date_end = kwargs["bought_date_end"] if "bought_date_end" in kwargs else None
        state = kwargs["state"] if "state" in kwargs else None

        headers = {
            "Content-Type": "application/json",
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        data = {
            "loginId": login_id,
            "enableTravelDateQuery": enable_travel_date_query,
            "enableBoughtDateQuery": enable_bought_date_query,
            "enableStateQuery": enable_state_query
        }

        if enable_travel_date_query:
            if travel_date_start is None or travel_date_end is None:
                return None
            data["travelDateStart"] = travel_date_start
            data["travelDateEnd"] = travel_date_end

        if enable_bought_date_query:
            if bought_date_start is None or bought_date_end is None:
                return None
            data["boughtDateStart"] = bought_date_start
            data["boughtDateEnd"] = bought_date_end

        if enable_state_query:
            if state is None:
                return None
            data["state"] = state

        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        resp = self.client.post(
            QuickStartUser.ORDER_SERVICE + "/order/refresh",
            headers=headers,
            json=data,
            context=context,
            timeout=REQUEST_TIMEOUT
        )

        if resp.json()["status"] == 1:
            orders = resp.json()["data"]

        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        resp = self.client.post(
            QuickStartUser.ORDER_OTHER_SERVICE + "/orderOther/refresh",
            headers=headers,
            json=data,
            context=context,
            timeout=REQUEST_TIMEOUT
        )

        if resp.json()["status"] == 1:
            order_others = resp.json()["data"]

        return orders, order_others

    def query_train_foods(self, date, start_station, end_station, trip_id):
        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        headers = {
            "Content-Type": "application/json",
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        resp = self.client.get(QuickStartUser.FOOD_SERVICE + "/foods/%s/%s/%s/%s" %
                               (date, start_station, end_station, trip_id),
                               context=context, headers=headers, timeout=REQUEST_TIMEOUT)
        try:
            resp_json_dict = resp.json()
        except:
            return None 
        
        if resp_json_dict["status"] == 1:
            return resp_json_dict["data"]
        else:
            return None

    def query_assurance_types(self):
        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        headers = {
            "Authorization": "Bearer " + self.user_token["fdse_microservice"],
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        resp = self.client.get(QuickStartUser.ASSURANCE_SERVICE + "/assurances/types",
                               context=context,
                               headers=headers,
                               timeout=REQUEST_TIMEOUT)
        resp_json_dict = resp.json()
        if resp_json_dict["status"] == 1:
            return resp_json_dict["data"]
        else:
            return None

    def query_contacts(self):
        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        headers = {
            "Authorization": "Bearer " + self.user_token["fdse_microservice"],
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        resp = self.client.get(QuickStartUser.CONTACTS_SERVICE + "/contacts/account/" + self.user_id["fdse_microservice"],
                               context=context,
                               headers=headers,timeout=REQUEST_TIMEOUT)
        resp_json_dict = resp.json()
        if resp_json_dict["status"] == 1:
            return resp_json_dict["data"]
        else:
            return None

    def preserve(self,
                 username: str,
                 contacts_id: str,
                 trip_id: str,
                 seat_type: int,
                 date: str,
                 from_station_name: str,
                 to_station_name: str,
                 assurance: int,
                 food_type: int,
                 food_name: str,
                 food_price: float,
                 food_station_name: str,
                 food_store_name: str,
                 enable_consignee: bool,
                 handle_date: str,
                 consignee_name: str,
                 consignee_phone: str,
                 consignee_weight: float):

        context = {
            "user_uuid": self.user_uuid,
            "start_time_ms": time.time() * 1000,
        }

        headers = {
            "Authorization": "Bearer " + self.user_token[username],
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }

        data = {
            "accountId": self.user_id[username],
            "contactsId": contacts_id,
            "tripId": trip_id,
            "seatType": seat_type,
            "date": date,
            "from": from_station_name,
            "to": to_station_name,
            "assurance": assurance,
            "foodType": food_type,
            "isWithin": False
        }

        if food_type != 0:
            data["foodName"] = food_name
            data["foodPrice"] = food_price

        if food_type == 2:
            data["stationName"] = food_station_name
            data["storeName"] = food_store_name

        if enable_consignee:
            if not (consignee_name is None or consignee_phone is None or handle_date is None):
                data["consigneeName"] = consignee_name
                data["consigneePhone"] = consignee_phone
                data["consigneeWeight"] = consignee_weight
                data["handleDate"] = handle_date

        self.client.post(
            QuickStartUser.PRESERVE_SERVICE + "/preserve",
            context=context,
            headers=headers,
            json=data,
            timeout=REQUEST_TIMEOUT
        )

    @task
    def main_task(self):
        if self.first:
            self.first = False 
            return 

        self.sync_info()

        """ 搜索一天之后的票 """
        year = str(self.local_time.tm_year)
        mon = ("0" + str(self.local_time.tm_mon)) if self.local_time.tm_mon < 10 else str(self.local_time.tm_mon)
        day = ("0" + str(self.local_time.tm_mday + 1)) if self.local_time.tm_mday + 1 < 10 else str(self.local_time.tm_mday + 1)
        date = year + "-" + mon + "-" + day

        """ 随机搜索次数 """
        search_time = random.randint(3, 5)
        
        for _ in range(search_time):
            ticket = self.search_travel_plan(
                'cheapest',
                self.start_station["station_name"],
                self.end_station["station_name"],
                date
            )
            
            if ticket is not None:
                _ = self.query_train_foods(
                    date,
                    ticket[0]["fromStationName"],
                    ticket[0]["toStationName"],
                    ticket[0]["tripId"]
                )

        # assurances_types = self.query_assurance_types()
        # contacts = self.query_contacts()
        # orders, order_others = self.query_orders(self.user_id["fdse_microservice"], False, False, False)

flag = False

class CustomShape(LoadTestShape):
    time_limit = len(RPS)
    spawn_rate = 100

    def tick(self):
        run_time = self.get_run_time()
        if run_time < self.time_limit:
            user_count = RPS[int(run_time)]
            return (user_count, self.spawn_rate)
        return None

