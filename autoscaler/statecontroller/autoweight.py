import time 
import math 
import os 
import json 
import threading 
from concurrent import futures
from concurrent.futures import as_completed
from typing import List, Dict 
from logging import Logger
from gevent.pywsgi import WSGIServer

from omegaconf import DictConfig
from kubernetes.client.rest import ApiException
from flask import Flask 
from flask_restful import Resource, Api 
import requests 

from . import Scaler
from ..util.locust import Locust  


# Labels 
GENESIS_IO_STATE_LABEL = 'swiftkube.io/state'
GENESIS_IO_ENDPOINT_LABEL = 'swiftkube.io/endpoint'
GENESIS_IO_IPVS_WEIGHT_LABEL = 'swiftkube.io/ipvs-weight'

GENESIS_IO_POD_STATE_RR = 'Ready-Running'
GENESIS_IO_POD_STATE_RFS = 'Ready-FullSpeed'
GENESIS_IO_POD_STATE_RCN = 'Ready-CatNap'
GENESIS_IO_POD_STATE_RLN = 'Ready-LongNap'
GENESIS_IO_POD_STATE_INIT = 'Initializing'

GENESIS_IO_ENDPOINT_UP = 'Up'
GENESIS_IO_ENDPOINT_DOWN = 'Down'

INIT_LABELS = {
    GENESIS_IO_STATE_LABEL: GENESIS_IO_POD_STATE_RR,
    GENESIS_IO_ENDPOINT_LABEL: GENESIS_IO_ENDPOINT_UP
}

ROOT_PATH = os.path.split(os.path.realpath(__file__))[0]

webapp = Flask('AutoWeightWebApp')
webapi = Api(webapp)


class AutoWeight(Scaler):
    def __init__(self, cfg: DictConfig, data_dir, logger: Logger):
        super().__init__(cfg, logger)
        self.__cfg = cfg 
        self.__scaler_cfg = cfg.scaler.autoweight  
        self.__logger = logger
        self.__logger.info('AutoWeight 初始化') 
        self.__priv_data_path = data_dir
        self.__workload = 'const_1000'
        self.__locust = None 
        self.__last_modify_ts = None 
        self.__watcher_sem = threading.Semaphore(value=0)
        self.__learn = self.__scaler_cfg.learn
        self.__explore_episode = 360 * 8  # 八个“NASA日”
        self.__weights = list()
        for weight in reversed(self.__scaler_cfg.weights):
            self.__weights.append(-weight)
        for weight in self.__scaler_cfg.weights:
            self.__weights.append(weight)
        self.__max_weight = self.__scaler_cfg.max_weight
        self.__webserver = None 
        self.__webserver_thread = None 
        
    def get_status(self):
        return dict(
            workload=self.__workload,
            run_mode='learn' if self.__learn else 'evaluate',
            weights=self.__weights
        ) 
        
    def locust_requests_on_modified(self, event):
        ts = time.time()
        if self.__last_modify_ts is not None:
            if ts - self.__last_modify_ts < 10:
                return 
        self.__last_modify_ts = ts
        self.__logger.info('requests.csv 发生变化')
        self.__watcher_sem.release()
    
    def pre_start(self):
        self.__locust = Locust(
            self.__cfg, f'csv-output-{self.get_date_string()}',
            self.__workload, self.__logger.getChild('AutoWeight-Locust')
        ) 
        if self.__learn:
            self.__logger.info('启动 locust 与 otelcol')
            self.__locust.start()
            self.__logger.info('注册 oberserver')
            self.__locust.register_observer_on_requests(
                self.locust_requests_on_modified)
            
        webapi.add_resource(
            ResourceStatus, 
            '/api/v1/status', 
            endpoint='status', 
            resource_class_kwargs=dict(
                autoweight=self
            ))
        # 启动 webserver
        self.__webserver = WSGIServer(('127.0.0.1', 8000), webapp)
        self.__webserver_thread = threading.Thread(
            target=self.__webserver.serve_forever,
            name='webserver'
        )
        self.__webserver_thread.start()
        
    def start(self):
        if self.__learn:
            self.explore_loop() 
    
    def explore_loop(self):
        self.__logger.info('探索 loop 启动')
        samples_file = f'explore_samples-{self.__cfg.enabled_service_config}-{self.__workload}-{self.get_date_string()}.csv'
        samples_file = open(self.__priv_data_path / samples_file, 'w+')
        samples_file.write('rps,p99_rt,p95_rt,p50_rt,mean_rt,action,action_p,allocation,avg_cpu_usage,overallocation_ratio\n')
        samples_file.flush()
        
        for episode in range(self.__explore_episode):
            
            self.__watcher_sem.acquire()  
            
    def set_pod_ipvs_weight(self, pod_dict: Dict, weight: int):
        try:
            _ = self.patch_k8s_pod(
                pod_dict["metadata"]["name"], pod_dict["metadata"]["namespace"],
                body=dict(metadata=dict(labels={GENESIS_IO_IPVS_WEIGHT_LABEL: f"{weight}"}))
            ) 
        except ApiException as e:
            if e.status == 404:
                self.__logger.error(
                    f'pod (name={pod_dict["metadata"]["name"]} '
                    f'namespace={pod_dict["metadata"]["namespace"]}) '
                    'not found'
                )
            else:
                self.__logger.error(str(e))
        
    def schedule_weight(self, pods: List[Dict], weights: List[int]):
        podname_to_weight = dict()
        for i, weight in enumerate(weights):
            podname_to_weight[pods[i]['metadata']['name']] = weight 
        return podname_to_weight    
        
    def calculate_weight_for_pods(self, pods: List[Dict], weight_idx: int) -> Dict:
        exponent = self.__weights[weight_idx]
        nr_pods = len(pods)
        weights = list()
        for i in range(nr_pods):
            index = i + 1
            x = index / nr_pods 
            if exponent >= 1:
                """ x^exponent """
                weights.append(int(math.pow(x, exponent) * self.__max_weight))
            elif exponent <= -1:
                """ -(-(x-1))^exponent+1 """
                weights.append(int((-math.pow(-(x-1), -exponent)+1) * self.__max_weight))
            else:
                raise Exception(f'exponent must <= -1 or >= 1, {exponent} is not allowed')
        return self.schedule_weight(pods, weights)
            
    def set_pods_ipvs_weight(self, service: str, weight_index: int):
        deploy_name = self.get_k8s_dep_name_from_cfg(service_name=service)
        namespace = self.get_k8s_namespace_from_cfg()
        pods = self.list_k8s_pods_for_deployment(namespace=namespace, name=deploy_name)
        if pods is None:
            return 
        podname_to_weight = self.calculate_weight_for_pods(pods, weight_index)
        ret_futures = list()
        with futures.ThreadPoolExecutor(max_workers=200) as executor:
            for pod in pods:
                podname = pod['metadata']['name']
                executor.submit(self.set_pod_ipvs_weight, pod, podname_to_weight[podname])
            for future in as_completed(ret_futures):
                future.result()
            
    def set_services_ipvs_weight(self, service_to_weight_index: Dict[str, int]):
        ret_futures = list()
        with futures.ThreadPoolExecutor(max_workers=200) as executor:
            for service, weight_index in service_to_weight_index.items():
                executor.submit(self.set_pod_ipvs_weight, service, weight_index)  
            for future in as_completed(ret_futures):
                future.result()


class ResourceStatus(Resource):
    def __init__(self, autoweight: AutoWeight):
        self.__aw = autoweight 
    
    def get(self):
        return self.__aw.get_status()