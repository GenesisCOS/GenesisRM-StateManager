import logging as log
import time
import random
import os 
import json 
from pathlib import Path

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
locust.stats.CSV_STATS_FLUSH_INTERVAL_SEC = 60
locust.stats.CURRENT_RESPONSE_TIME_PERCENTILE_WINDOW = 60
locust.stats.PERCENTILES_TO_REPORT = [0.50, 0.80, 0.90, 0.95, 0.98, 0.99, 0.995, 0.999, 1.0]

setup_logging("INFO", None)

DATA_DIR = os.getenv('DATA_DIR')
if DATA_DIR is not None:
    request_log_file = open(f'{DATA_DIR}/request.log', 'a') 
else:
    request_log_file = open('request.log', 'a')
    
WAIT_TIME = os.getenv('WAIT_TIME')
if WAIT_TIME is not None:
    WAIT_MIN = float(WAIT_TIME)
    WAIT_MAX = int(WAIT_TIME)

HOST = os.getenv('TARGET_HOST')
DATASET = os.getenv('DATASET')

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
    "service.name": "locust",
    "service.version": "0.0.1"
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

class QuickStartUser(HttpUser):
    host = HOST 
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
        
    @events.request.add_listener
    def on_request(response_time, context, name, **kwargs):
        global tracer 
        
        request_log_file.write(json.dumps({
            'time': time.perf_counter(),
            'latency': response_time / 1e3,
            'context': context,
            'name': name
        }) + '\n')

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

        resp = self.client.get(QuickStartUser.VERIFY_CODE_URL, context=context, headers=headers)
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
            context=context
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
        resp = self.client.get(QuickStartUser.TRAIN_SERVICE_URL + "/trains",
                               context=context, headers=headers)
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
                               context=context, headers=headers)
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
                context=context
            )

        resp_json_dict = resp.json()
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
            context=context
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
            context=context
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
                               context=context, headers=headers)
        resp_json_dict = resp.json()
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
                               headers=headers)
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
                               headers=headers)
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
            json=data
        )

    # @task
    def test(self):
        headers = {
            "Authorization": "Bearer " + self.user_token["admin"],
            "requestSendTimeNano": "{:.0f}".format(time.time() * 1000000000),
            "cluster": CLUSTER
        }
        data = {
            "userName": "ace",
            "password": "222222",
            "gender": 1,
            "documentType": 1,
            "documentNum": "2135488099312X",
            "email": "ace@163.com"
        }
        print(self.client.post("/api/v1/adminuserservice/users",
                               headers=headers, json=data).content)
        users = self.client.get("/api/v1/adminuserservice/users", headers=headers).json()["data"]
        print(len(users))
        num = 0
        for user in users:
            print(user["userName"])
            time.sleep(0.3)
            if user["userName"][-1] == "\n":
                resp_delete = self.client.delete("/api/v1/adminuserservice/users/" + user["userId"],
                                                 headers=headers)
                print(resp_delete.content)
                num += 1
        print(num)
        self.stop()

    @task
    def test_task(self):
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

