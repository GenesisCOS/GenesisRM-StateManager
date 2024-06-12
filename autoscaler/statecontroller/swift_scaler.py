import time 
import random 
import os 
import math 
import json 
import copy 
import threading 
import logging 
import pickle
import traceback 
import pathlib 
from concurrent import futures
from concurrent.futures import as_completed
from typing import List, Dict, Tuple 
from logging import Logger

import requests 
from kafka import KafkaConsumer
from kubernetes.client import V1Pod
from kubernetes.client.rest import ApiException
import numpy as np 
import pandas as pd 

from . import Scaler
from ..ts_predictor.enbpi import EnbpiPredictor

# Labels 
GENESIS_IO_STATE_LABEL = 'swiftkube.io/state'
GENESIS_IO_ENDPOINT_LABEL = 'swiftkube.io/endpoint'

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

TOKEN = 1

OUTPUT = False 

class PandasDataset(object):
    def __init__(self, 
                 timestamp_col: str,
                 max_length: int) -> None:
        self.__lock = threading.Lock()
        self.__data = None 
        self.__ts_col = timestamp_col
        self.__max_length = max_length
        
    def append(self, data: Dict) -> None:
        """ Append and sort by timestamp column """
        df = pd.DataFrame(data)
        df.set_index(self.__ts_col, inplace=True)
        
        self.__lock.acquire()
        
        if self.__data is None:
            self.__data = df 
        else:
            self.__data = pd.concat([self.__data, df]) 
            self.__data.sort_index(inplace=True)
        
        if self.__data is not None:
            if len(self.__data) > self.__max_length:
                delta = len(self.__data) - self.__max_length
                self.__data.drop(self.__data.head(delta).index, inplace=True)
        
        self.__lock.release()
        
    def get_mean_value(self, col: str):
        self.__lock.acquire()
        
        if self.__data is None:
            self.__lock.release()
            return 0
        
        values = self.__data[col].values
        upper_val = np.mean(values) + 3 * np.std(values)
        max_val = np.max(values)
        retval = min(max_val, upper_val)
        
        self.__lock.release()
        return retval
    

def swift_list_pods_of_dep(namespace, label, value):
    
    resp = requests.post(
        'http://localhost:10000/pods/lister/',
        json=dict(
            namespace=namespace,
            label=label,
            value=value
        ),
        headers={
            'Connection': 'close',
        }
    )
    return json.loads(resp.text)
   

class SwiftKubeScaler(Scaler):
    def __init__(self, cfg, data_dir: pathlib.Path, logger: Logger):
        super().__init__(cfg, logger)
        
        self.__cfg = cfg 
        self.__logger = logger
        
        self.__priv_data_dir = data_dir
        
        self.__kafka_consumer = None 
        self.__kafka_data: Dict[str, Dict[str, PandasDataset]] = dict()
        
        self.min_cpu_limit = cfg.scaler.swiftkube_scaler.min_cpu_limit
        self.strategy = cfg.scaler.swiftkube_scaler.strategy
        
        self.last_st_controller_ts = None 
        self.last_lt_controller_ts = None 
        
        self.__service_endpoints_map: Dict[str, List[str]] = dict()
        self.__services = self.get_all_services_from_cfg()
        
        self.__st_predictors: Dict[str, Dict[str, EnbpiPredictor]] = dict()
        
        self.replicas: Dict[str, Dict[str, int]] = dict()
        
        self.__rt_l2_prev_result = None 
        self.__rt_l1_l2_prev_result = None 
        
        self.__lt_pred_result = None 
        self.prev_warmup_ts = time.time()
        
        self.__sync_replicas_future = None 
        self.__sync_executor = futures.ThreadPoolExecutor()
        
        # Init replicas and kafka data 
        for service in self.__services:
            
            self.init_running = self.get_service_max_replicas_from_cfg(service)
            self.init_replicas = self.get_service_max_replicas_from_cfg(service)
            self.init_l1s = 0
            self.init_l2s = 0
            
            self.replicas[service] = dict(
                replicas=self.init_replicas,
                running=self.init_running,
                l1_sleep=self.init_l1s,
                l2_sleep=self.init_l2s
            )
            
            endpoints = self.get_service_endpoints(service)
            self.__service_endpoints_map[service] = endpoints
            
            for endpoint in endpoints:
                if service not in self.__kafka_data:
                    self.__kafka_data[service] = dict()
                if endpoint not in self.__kafka_data[service]:
                    self.__kafka_data[service][endpoint] = \
                        PandasDataset('timestamp', 20)
        
        # Init EnbpiPredictor 
        for service, endpoints in self.__service_endpoints_map.items():
            for endpoint in endpoints:
                if service not in self.__st_predictors:
                    self.__st_predictors[service] = dict()
                if endpoint not in self.__st_predictors[service]:
                    __l = self.__logger.getChild(f'StreamEnbPI-{service}-{endpoint}')
                    self.__st_predictors[service][endpoint] = \
                        EnbpiPredictor(
                            self.__cfg, __l, 
                            service, endpoint,
                            '(span_count * (rt_mean / 1000))',
                            agg_function='mean'
                        )
    
    def kafka_consumer(self):
        self.__kafka_consumer = KafkaConsumer(
            self.__cfg.scaler.swiftkube_scaler.kafka.topic,
            bootstrap_servers=self.__cfg.scaler.swiftkube_scaler.kafka.bootstrap_servers 
        )
        
        def __append_data(msg):
            value = json.loads(msg.value.decode()) 
            service_name = value['metadata']['serviceName']
            endpoint_name = value['metadata']['endpointName']
            data = dict(
                concurrency=[value['concurrency']],
                timestamp=[int(value['windowEndUnixTimestamp'] / 1000)]
            )
            __append_start = time.time()
            
            self.__kafka_data[service_name][endpoint_name].append(data)
        
        with futures.ThreadPoolExecutor(max_workers=40) as executor:
            for msg in self.__kafka_consumer:
                executor.submit(__append_data, msg)
        
    def pre_start(self):
        self.__logger.info('GenesisRM preStart ...')
        self.__lt_logger = self.__logger.getChild('LongTermPred')
        
        if self.__cfg.locust.workload == 'nasa':
            data_path = self.__cfg.scaler.swiftkube_scaler.nasa_lt_result
            if os.path.exists(data_path): 
                self.__lt_logger.info('already trained.')
                with open(data_path, 'rb') as data_file:
                    self.__lt_pred_result = pickle.load(data_file)
            else:
                raise Exception('lt_result_nasa.pkl not exists')
        
        elif self.__cfg.locust.workload == 'fluctuating':
            data_path = self.__priv_data_dir / 'lt_result_eclog.pkl'
            if os.path.exists(data_path): 
                self.__lt_logger.info('already trained.')
                with open(data_path, 'rb') as data_file:
                    self.__lt_pred_result = pickle.load(data_file)
            else:
                raise Exception('lt_result_eclog.pkl not exists')
        return True 

    def start(self):
        self.__logger.info('GenesisRM start ...')
        __cl_logger = self.__logger.getChild('ControlLoop')
        
        # Start kafka consumer 
        threading.Thread(target=self.kafka_consumer, daemon=True).start()
        
        self.locust_start_time = time.time()
        time.sleep(5)
        
        if self.strategy.startswith('SGS'):
            lt_controller_result = self.lt_controller()
            result = dict()
            # For each service 
            for service in self.__services:
                result[service] = dict()
                
                lt_ret = lt_controller_result.get(service)
                
                result[service]['replicas'] = lt_ret 
                result[service]['running'] = lt_ret 
                result[service]['l1_sleep'] = 0
                result[service]['l2_sleep'] = 0
            
            for service, conf in result.items():
                self.replicas[service] = conf 
        
        # First sync replcas 
        self.sync_replicas()
        
        wait_sec = 180
        self.__logger.info(f'Wait for {wait_sec} seconds.')
        time.sleep(wait_sec) 
        
        # Init short-term workload predictors 
        self.__logger.info('Initializing ST predictors ...')
        with futures.ThreadPoolExecutor(max_workers=40) as executor:
            ret_futures = list()
            for _, endpoints in self.__st_predictors.items():
                for _, predictor in endpoints.items():
                    future = executor.submit(predictor.init)
                    ret_futures.append(future)
            
            for future in as_completed(ret_futures):
                future.result()
            self.last_st_controller_ts = time.time()
        self.__logger.info('initialize ST predictors done')

        # Main control loop 
        while True:
            control_loop_start = time.time()
            
            #==================== horizontal autoscaling ====================# 
            
            if self.strategy == 'horizontal':
                
                lt_controller_result = self.lt_controller()
                result = dict()
                # For each service 
                for service in self.__services:
                    result[service] = dict()
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    
                    lt_ret = lt_controller_result.get(service)
                    lt_ret = min(lt_ret, max_replicas)
                    
                    if self.__rt_l2_prev_result is not None:
                        prev_replicas_up_ts = self.__rt_l2_prev_result[service]['__replicas_up_ts__']
                        prev_replicas_down_ts = self.__rt_l2_prev_result[service]['__replicas_down_ts__']
                        prev_replicas = self.__rt_l2_prev_result[service]['replicas']
                    else:
                        prev_replicas_up_ts = 0
                        prev_replicas_down_ts = 0
                        prev_replicas = self.init_replicas
                    
                    # Replicas     
                    up_ts_delta = time.time() - prev_replicas_up_ts
                    down_ts_delta = time.time() - prev_replicas_down_ts
                    
                    replicas = lt_ret 
                    replicas = max(replicas, 1)
                    
                    # 最多删除2个
                    #replicas = max(prev_replicas - 2, replicas)
                    
                    if replicas < prev_replicas:
                        # 最多每60秒缩容一次
                        if down_ts_delta <= 60:
                            replicas = prev_replicas 
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                        else:
                            result[service]['__replicas_down_ts__'] = time.time()
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    elif replicas > prev_replicas:
                        # 最多每15秒横向扩容一次
                        if up_ts_delta < 15:
                            replicas = prev_replicas
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                        else:
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = time.time()
                    else:
                        result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                        result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    result[service]['replicas'] = replicas 
                    
                    # Running Replicas 
                    result[service]['running'] = replicas 
                    
                    # Level1 Sleep 
                    result[service]['l1_sleep'] = 0
                    
                    # Level2 Sleep 
                    result[service]['l2_sleep'] = 0
                
                self.__rt_l2_prev_result = result 
                
                for service, conf in result.items():
                    if service.startswith('__'):
                        continue
                    self.replicas[service] = conf 
            
            #================== GenesisRM-SG-L1 (rt-l2-v2) or GenesisRM-SGS-L1 (rt-l2) =============#
            if self.strategy == 'SGS-L1' or self.strategy == 'SG-L1':
                
                # Run controllers 
                __run_controller_start = time.perf_counter()

                st_controller_result = self.st_controller()
                rt_controller_result = self.rt_controller()
                
                if self.strategy == 'SGS-L1':
                    lt_controller_result = self.lt_controller()
                    
                __run_controller_time = time.perf_counter() - __run_controller_start
                __cl_logger.info(f'Run controllers use {__run_controller_time}')
                
                result = dict()
                
                # For each service 
                for service in self.__services:
                    result[service] = dict()
                    
                    rt_ret = rt_controller_result.get(service)
                    st_ret = st_controller_result.get(service)
                    if self.strategy == 'SGS-L1':
                        lt_ret = lt_controller_result.get(service)
                    
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    rt_ret = min(rt_ret, max_replicas)
                    st_ret = min(st_ret, max_replicas)
                    if self.strategy == 'SGS-L1':
                        lt_ret = min(lt_ret, max_replicas)
                    
                    if self.__rt_l2_prev_result is not None:
                        prev_replicas_up_ts = self.__rt_l2_prev_result[service]['__replicas_up_ts__']
                        prev_replicas_down_ts = self.__rt_l2_prev_result[service]['__replicas_down_ts__']
                        prev_replicas = self.__rt_l2_prev_result[service]['replicas']
                    else:
                        prev_replicas_up_ts = 0
                        prev_replicas_down_ts = 0
                        prev_replicas = self.init_replicas
                    
                    # Replicas     
                    up_ts_delta = time.time() - prev_replicas_up_ts
                    down_ts_delta = time.time() - prev_replicas_down_ts
                    if self.strategy == 'SGS-L1':
                        replicas = max(st_ret, lt_ret)
                    else:
                        replicas = st_ret 
                    replicas = max(replicas, 1)
                    
                    # 最多删除三个
                    replicas = max(prev_replicas - 2, replicas)
                    
                    if replicas < prev_replicas:
                        # 最多每60秒缩容一次
                        if down_ts_delta <= 60:
                            replicas = prev_replicas 
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                        else:
                            result[service]['__replicas_down_ts__'] = time.time()
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    elif replicas > prev_replicas:
                        # 最多每五秒横向扩容一次
                        if up_ts_delta < 5:
                            replicas = prev_replicas
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                        else:
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = time.time()
                    else:
                        result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                        result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    result[service]['replicas'] = replicas 
                    
                    # Running Replicas 
                    running_replicas = min(replicas, rt_ret)
                    result[service]['running'] = running_replicas
                    
                    # Level1 Sleep 
                    result[service]['l1_sleep'] = replicas - running_replicas
                    
                    # Level2 Sleep 
                    result[service]['l2_sleep'] = 0
                
                self.__rt_l2_prev_result = result 
                
                for service, conf in result.items():
                    if service.startswith('__'):
                        continue
                    self.replicas[service] = conf 
            
            #================== GenesisRM-SGS-L2 =========================#
            if self.strategy == 'SGS-L2':
                
                # Run controllers 
                __run_controller_start = time.time()

                st_controller_result = self.st_controller()
                rt_controller_result = self.rt_controller()
                lt_controller_result = self.lt_controller()
                    
                __run_controller_time = time.time() - __run_controller_start
                __cl_logger.info(f'Run controllers use {__run_controller_time}s')
                
                result = dict()
                
                if self.__rt_l1_l2_prev_result is not None:
                    prev_token_update_ts = self.__rt_l1_l2_prev_result['__token_update_ts__']
                    token = self.__rt_l1_l2_prev_result['__token__']
                else:
                    prev_token_update_ts = 0
                    token = TOKEN 
                    
                # For each service 
                for service in self.__services:
                    result[service] = dict()
                    
                    rt_ret = rt_controller_result.get(service)
                    st_ret = st_controller_result.get(service)
                    lt_ret = lt_controller_result.get(service)
                    
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    rt_ret = max(min(rt_ret, max_replicas), 1)
                    st_ret = max(min(st_ret, max_replicas), 1)
                    lt_ret = max(min(lt_ret, max_replicas), 1)
                    
                    if self.__rt_l1_l2_prev_result is not None:
                        prev_replicas_up_ts = self.__rt_l1_l2_prev_result[service]['__replicas_up_ts__']
                        prev_replicas_down_ts = self.__rt_l1_l2_prev_result[service]['__replicas_down_ts__']
                        prev_replicas = self.__rt_l1_l2_prev_result[service]['replicas']
                        prev_s1 = self.__rt_l1_l2_prev_result[service]['l1_sleep']
                        prev_running = self.__rt_l1_l2_prev_result[service]['running']
                    else:
                        prev_replicas_up_ts = 0
                        prev_replicas_down_ts = 0
                        prev_replicas = self.init_replicas
                        prev_s1 = self.init_l1s
                        prev_running = self.init_running
                    
                    # Replicas     
                    up_ts_delta = time.time() - prev_replicas_up_ts
                    down_ts_delta = time.time() - prev_replicas_down_ts
                    replicas = lt_ret 
                    
                    if replicas < prev_replicas:
                        # 最多每60秒横向缩容一次
                        if down_ts_delta > 15 and token > 0:
                            result[service]['__replicas_down_ts__'] = time.time()
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                            token -= 1
                        else:
                            replicas = prev_replicas 
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    elif replicas > prev_replicas:
                        # 最多每15秒横向扩容一次
                        if up_ts_delta > 15 and token > 0:
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = time.time()
                            token -= 1
                        else:
                            replicas = prev_replicas
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    else:
                        result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                        result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    
                    result[service]['replicas'] = replicas 
                    
                    # Running
                    running_replicas = min(replicas, rt_ret)
                    result[service]['running'] = running_replicas
                    
                    # Level1-Suspended
                    l1_sleep_replicas = max(min(st_ret, lt_ret) - running_replicas, 0)
                    if l1_sleep_replicas == 0:
                        l1_sleep_replicas = max(0, prev_running + prev_s1 - running_replicas)
                        l1_sleep_replicas = min(replicas - running_replicas, l1_sleep_replicas)
                    # TODO 最多3个L1S pod
                    #l1_sleep_replicas = min(l1_sleep_replicas, 3)
                    result[service]['l1_sleep'] = l1_sleep_replicas
                    
                    # Level2-Suspended
                    l2_sleep_replicsa = max(0, replicas - running_replicas - l1_sleep_replicas)
                    result[service]['l2_sleep'] = l2_sleep_replicsa
                    
                result['__token_update_ts__'] = prev_token_update_ts
                if time.time() - prev_token_update_ts >= 15:
                    token += 1
                    token = min(TOKEN, token)
                    result['__token_update_ts__'] = time.time()
                result['__token__'] = token 
                
                self.__rt_l1_l2_prev_result = result 
                
                for service, conf in result.items():
                    if service.startswith('__'):
                        continue
                    self.replicas[service] = conf 
            
            #================== GenesisRM-SG-L2 ==========================#
            elif self.strategy == 'SG-L2':
                
                # Run controllers 
                __run_controller_start = time.time()
                
                st_controller_result = self.st_controller()
                rt_controller_result = self.rt_controller()
                    
                __run_controller_time = time.time() - __run_controller_start
                __cl_logger.info(f'Run controllers use {__run_controller_time}s')
                
                result = dict()
                
                # For each service ...
                for service in self.__services:
                    rt_ret = rt_controller_result.get(service)
                    
                    if st_controller_result is not None:
                        st_ret = st_controller_result.get(service)
                    else:
                        st_ret = -1
                    
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    
                    # Fix result 
                    running = min(rt_ret, max_replicas)
                    #if self.__rt_l1_prev_result is not None:
                    #    prev_running = self.__rt_l1_prev_result[service]['running']
                    #    rt_ret = min(rt_ret, prev_running + 1)
                        
                    if st_ret >= 0:
                        st_ret = min(st_ret, max_replicas)
                    
                    if st_ret >= 0:
                        l1s = max(st_ret - running, 0)
                    else:
                        l1s = 0
                    
                    result[service] = dict(
                        replicas=max_replicas,
                        running=running,
                        l1_sleep=l1s,
                        l2_sleep=max_replicas - running - l1s
                    )
                
                # Update replicas 
                for service, conf in result.items():
                    if service.startswith('__'):
                        continue
                    self.replicas[service] = conf 
            
            #================== GenesisRM-S-L1 ==========================# 
            elif self.strategy == 'S-L1':
                # Run RT-controller 
                rt_controller_result = self.rt_controller()
                
                result = dict()
                
                # For each service ...
                for service in self.__services:
                    rt_ret = rt_controller_result.get(service)
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    rt_ret = min(rt_ret, max_replicas)
                    rt_ret = max(1, rt_ret)
                    
                    result[service] = dict(
                        replicas=max_replicas,
                        running=rt_ret,
                        l1_sleep=max_replicas - rt_ret,
                        l2_sleep=0
                    ) 
                
                # Update replicas 
                for service, conf in result.items():
                    if service.startswith('__'):
                        continue
                    self.replicas[service] = conf 
                
            # Sync replcas 
            __sync_replicas_start = time.time()
            if (self.__sync_replicas_future is not None) and \
                    (not self.__sync_replicas_future.done()):
                self.__sync_replicas_future.result()
            
            self.__sync_replicas_future = \
                self.__sync_executor.submit(self.sync_replicas)
            
            __cl_logger.info(f'sync replicas use {time.time() - __sync_replicas_start}s')
            
            control_loop_time = time.time() - control_loop_start
            __cl_logger.info(f'control loop use {control_loop_time}s')
            
            if control_loop_time < 5:
                time.sleep(5 - control_loop_time)
    
    def rt_controller(self) -> Dict[str, int]:
        
        __rt_start = time.time()
        
        __rt_logger = self.__logger.getChild('RTController')
        __rt_logger.debug('Running ...')
        
        def __get_concurrency(service_name) -> Tuple[str, int]:
            retval = 0
            for endpoint in self.__service_endpoints_map[service_name]:
                retval += self.__kafka_data[service_name][endpoint].get_mean_value('concurrency')
            return service_name, retval 
        
        def __get_max_cpu_usage(service_name) -> Tuple[str, int]:
            return service_name, np.max(self.fetch_cpu_usage_data(service_name)) * 1000
        
        retval = dict()
        ret_futures = list()
        
        with futures.ThreadPoolExecutor() as executor:
            for service in self.__services:
                future = executor.submit(__get_concurrency, service) 
                #future = executor.submit(__get_max_cpu_usage, service) 
                ret_futures.append(future)
                
            for future in as_completed(ret_futures):
                result = future.result()
                retval[result[0]] = result[1]
        
        for service_name, result in retval.items():
            # Concurrency 
            max_worker = self.get_service_max_worker_from_cfg(service_name)
            target = self.get_service_worker_target_utilization_from_cfg(service_name)
            threshold = max_worker * (target / 100)
            retval[service_name] = math.ceil(result / threshold)
            """
            # CPU Usage 
            cpu_request = 0
            resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
            for config in resources_config:
                if 'requests' in config['resources']:
                    if 'cpu' in config['resources']['requests']:
                        cpu_request += int(config['resources']['requests']['cpu'][:-1])
            threshold = cpu_request * 0.65
            retval[service_name] = math.ceil(result / threshold)
            """
        
        __rt_logger.debug(f'use {time.time() - __rt_start}s.')
        return retval 
    
    def st_controller(self) -> Dict[str, int]:
        retval = dict()
        __l = self.__logger.getChild('STController')
        __l.info('Running ...')
        __start = time.time()
        
        if time.time() - self.last_st_controller_ts > 60:
            self.__refit_st_predictors()
            self.last_st_controller_ts = time.time()
        
        result = dict()
        pred_result = self.st_predict_concurrency()
        
        for pred in pred_result:
            service = pred['service_name']
            concurrency = pred['predict_result']['result']['upper_bound']
            if service not in result:
                result[service] = concurrency
            else:
                result[service] = result[service] + concurrency
        
        for service, concurrency in result.items():
            max_worker = self.get_service_max_worker_from_cfg(service)
            
            target = self.get_service_worker_target_utilization_from_cfg(service)
            threshold = max_worker * (target / 100)
            
            retval[service] = math.ceil(concurrency / threshold)
            
        __l.info(f'use {time.time() - __start}s')
        return retval 
    
    def lt_controller(self) -> Dict[str, int]:
        retval = dict()
        __lt_start = time.time()
        
        __l = self.__logger.getChild('LTController')
        __l.info('Running ...')
        
        idx = int((__lt_start - self.locust_start_time))

        for dic in self.__lt_pred_result:
            service = dic['service_name']
            service += '-service'
            predicted = dic['predict_result'][idx] * 1000
            
            cpu_request = 0
            resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service))
            for config in resources_config:
                if 'requests' in config['resources']:
                    if 'cpu' in config['resources']['requests']:
                        cpu_request += int(config['resources']['requests']['cpu'][:-1])
                        
            threshold = cpu_request * 0.6
            
            print(f'{service} predicted: {predicted} threshold: {threshold} retval: {math.ceil(predicted / threshold)}')
            retval[service] = math.ceil(predicted / threshold)
        return retval
    
    def __refit_st_predictors(self):
        for _, endpoints in self.__st_predictors.items():
            for _, predictor in endpoints.items():
                predictor.refit()
    
    def __st_predict_concurrency(self, service_name, endpoint_name):
        result = dict(
            service_name=service_name,
            endpoint_name=endpoint_name
        )
        
        retval = self.__st_predictors[service_name][endpoint_name].predict()
        result['predict_result'] = retval 
        
        return result 
            
    def st_predict_concurrency(self):
        ret_futures = list()
        results = list()
        
        with futures.ThreadPoolExecutor() as executor:
            for service in self.get_all_services_from_cfg():
                endpoints = self.get_service_endpoints(service)
                for endpoint in endpoints:
                    future = executor.submit(self.__st_predict_concurrency, service, endpoint)
                    ret_futures.append(future)
        
            for future in as_completed(ret_futures):
                results.append(future.result())

        return results 
    
    def fetch_cpu_usage_data(self, service_name):
        
        end = time.time()
        start = end - 10
        dep_name = self.get_k8s_dep_name_from_cfg(service_name)
        namespace = self.get_k8s_namespace_from_cfg()
        
        retval = self.get_cpu_usage_from_prom(
            dep_name=dep_name, 
            namespace=namespace,
            start=start,
            end=end)
        
        return retval['value'].values 
    
    def set_pod_state_to_rr(self, service_name: str, pod_dict: Dict):
        __logger = self.__logger.getChild('Operation')
        
        try:
            pod = self.patch_k8s_pod(
                pod_dict["metadata"]["name"], pod_dict["metadata"]["namespace"],
                body=dict(
                    metadata=dict(
                        labels={
                            GENESIS_IO_STATE_LABEL: GENESIS_IO_POD_STATE_RR,
                            GENESIS_IO_ENDPOINT_LABEL: GENESIS_IO_ENDPOINT_UP
                        },
                        annotations={'controller.kubernetes.io/pod-deletion-cost': "10000"}
                    ),
                    spec=dict(
                        containers=copy.deepcopy(self.get_resources_config_from_cfg(service_name))
                    )
                )
            ) 
        except ApiException as e:
            if e.status == 404:
                __logger.error(
                    f'pod (name={pod_dict["metadata"]["name"]} '
                    f'namespace={pod_dict["metadata"]["namespace"]}) '
                    'not found'
                ) 
            time.sleep(0.5)
                
        __logger.info(
            f'set pod (name={pod.metadata.name} '
            f'namespace={pod.metadata.namespace}) '
            'to Ready-Running stats.'
        )
    
    def set_pod_state_to_rcn(self, service_name, pod: Dict):
        __l = self.__logger.getChild('Operation')
        
        resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
        for config in resources_config:
            if 'limits' in config['resources']:
                if 'cpu' in config['resources']['limits']:
                    config['resources']['limits']['cpu'] = '3000m'
            if 'requests' in config['resources']:
                if 'cpu' in config['resources']['requests']:
                    config['resources']['requests']['cpu'] = '10m' 
        
        try:
            pod = self.patch_k8s_pod(
                pod["metadata"]["name"], pod["metadata"]["namespace"],
                body=dict(
                    metadata=dict(
                        labels={
                            GENESIS_IO_STATE_LABEL: GENESIS_IO_POD_STATE_RCN,
                            GENESIS_IO_ENDPOINT_LABEL: GENESIS_IO_ENDPOINT_DOWN
                        },
                        annotations={
                            'controller.kubernetes.io/pod-deletion-cost': "5000"
                        }
                    ),
                    spec={
                        'containers': resources_config
                    }
                )
            )
        except Exception as e:
            __l.debug(f'set pod to L1Suspended stats failed.')
        __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to L1Suspended stats.')
    
    def set_pod_state_to_rln(self, service_name, pod: Dict):
        __l = self.__logger.getChild('Operation')
        
        resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
        for config in resources_config:
            if 'limits' in config['resources']:
                if 'cpu' in config['resources']['limits']:
                    pass  # TODO 
                if 'memory' in config['resources']['limits']:
                    pass  # TODO 
            if 'requests' in config['resources']:
                if 'cpu' in config['resources']['requests']:
                    config['resources']['requests']['cpu'] = '10m' 
                if 'memory' in config['resources']['requests']:
                    config['resources']['requests']['memory'] = '100Mi'  # TODO 
        try:
            pod = self.patch_k8s_pod(
                pod["metadata"]["name"], pod["metadata"]["namespace"],
                body=dict(
                    metadata=dict(
                        labels={
                            GENESIS_IO_STATE_LABEL: GENESIS_IO_POD_STATE_RLN,
                            GENESIS_IO_ENDPOINT_LABEL: GENESIS_IO_ENDPOINT_DOWN
                        },
                        annotations={
                            'controller.kubernetes.io/pod-deletion-cost': "0"
                        }
                    ),
                    spec=dict(containers=resources_config)
                )
            )
        except Exception as e:
            __l.debug(f'set pod to L2Suspended stats failed.')
        __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to L2Suspended stats.')
        
    def choose_pod_to_l2s(self, pods: List[V1Pod],
                          delta: int) -> List[V1Pod]:
        ret_pods = list()
        if len(pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < delta and len(pods) > 0:
            ret_pods.append(pods.pop(0))
            
        return ret_pods 
        
    def choose_pod_to_l1s(self,
                          pods: List[V1Pod],
                          delta: int) -> List[V1Pod]:
        ret_pods = list()
        if len(pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < delta and len(pods) > 0:
            ret_pods.append(pods.pop(0))
            
        return ret_pods 
    
    def choose_pod_to_running(self,  
                              s1_pods: List[Dict], 
                              s2_pods: List[Dict],
                              init_pods: List[Dict],
                              delta: int) -> List[Dict]:
        ret_pods = list()
        warm = 0
        
        while len(ret_pods) < delta and len(s1_pods) > 0:
            ret_pods.append(s1_pods.pop(0))
        if len(ret_pods) == delta:
            return ret_pods, warm
        else:
            while len(ret_pods) < delta and len(s2_pods) > 0:
                ret_pods.append(s2_pods.pop(0))
            if len(ret_pods) == delta:
                return ret_pods, warm 
            else:
                while len(ret_pods) < delta and len(init_pods) > 0:
                    warm += 1
                    ret_pods.append(init_pods.pop(0))
                
        return ret_pods, warm 
    
    def __do_sync_replicas(self, service, replicas_conf):
        
        replicas = replicas_conf.get('replicas') 
        running_replicas = replicas_conf.get('running')
        l1_sleep_replicas = replicas_conf.get('l1_sleep')
        
        dep_name = self.get_k8s_dep_name_from_cfg(service)
        namespace = self.get_k8s_namespace_from_cfg()
        
        dep_obj = self.get_k8s_deployment(dep_name, namespace)
        
        if dep_obj.spec.replicas != replicas: 
            dep_obj = self.set_k8s_deployment_replicas(dep_name, namespace, replicas)
        
        match_labels = list()
        for k, v in dep_obj.spec.selector.match_labels.items():
            match_labels.append((k, v)) 
        resp = swift_list_pods_of_dep(namespace, match_labels[0][0], match_labels[0][1])
        if resp['status'] != 'success':
            raise Exception(resp.reason)
        
        pods = resp['pods'] 
        
        new_pods = list()
        for pod in pods:
            if pod['status']['phase'] == 'Running':
                new_pods.append(pod)
        pods = new_pods 
        
        running_pods, l1s_pods, l2s_pods, initializing_pods = \
            list(), list(), list(), list()
        for pod in pods:
            labels = pod['metadata']['labels']
            if GENESIS_IO_STATE_LABEL not in labels:
                raise Exception(
                    f'Pod(name={pod["metadata"]["name"]} namespace={pod["metadata"]["namespace"]}) '
                    f'do not has {GENESIS_IO_STATE_LABEL} label.'
                )
            # Running pod 
            elif labels[GENESIS_IO_STATE_LABEL] == GENESIS_IO_POD_STATE_RR:
                running_pods.append(pod)
            # Sleeping (Level1) pod 
            elif labels[GENESIS_IO_STATE_LABEL] == GENESIS_IO_POD_STATE_RCN:
                l1s_pods.append(pod)
            # Sleeping (Level2) pod 
            elif labels[GENESIS_IO_STATE_LABEL] == GENESIS_IO_POD_STATE_RLN:
                l2s_pods.append(pod)
            # Initializing pod 
            elif labels[GENESIS_IO_STATE_LABEL] == GENESIS_IO_POD_STATE_INIT:
                initializing_pods.append(pod)
            else:
                raise Exception(f'Unknown label {GENESIS_IO_STATE_LABEL}={labels[GENESIS_IO_STATE_LABEL]}.')
        
        ret_futures = list()
        with futures.ThreadPoolExecutor(max_workers=200) as executor:
            warm = 0
            # Need more running pods 
            if len(running_pods) < running_replicas:
                delta_to_run = running_replicas - len(running_pods)
                pods_to_run, warm = self.choose_pod_to_running(
                    l1s_pods, 
                    l2s_pods, 
                    initializing_pods, 
                    delta_to_run
                )

                for pod in pods_to_run:
                    future = executor.submit(self.set_pod_state_to_rr, service, pod)
                    ret_futures.append(future)
                    
                running_pods += pods_to_run 
            
            # Need less running pods 
            elif len(running_pods) > running_replicas:
                delta = len(running_pods) - running_replicas
                pods_to_l1_sleep = self.choose_pod_to_l1s(running_pods, delta) 
                
                for pod in pods_to_l1_sleep:
                    future = executor.submit(self.set_pod_state_to_rcn, service, pod)
                    ret_futures.append(future)
                    
                l1s_pods += pods_to_l1_sleep 
            
            # Need more s1 pods 
            if len(l1s_pods) < l1_sleep_replicas:
                delta = l1_sleep_replicas - len(l1s_pods)
                pods_to_l1_sleep = self.choose_pod_to_l1s(l2s_pods, delta)
                
                for pod in pods_to_l1_sleep:
                    future = executor.submit(self.set_pod_state_to_rcn, service, pod)
                    ret_futures.append(future)
                
                l1s_pods += pods_to_l1_sleep 
            
            # Need less s1 pods 
            elif len(l1s_pods) > l1_sleep_replicas:
                delta = len(l1s_pods) - l1_sleep_replicas
                pods_to_l2_sleep = self.choose_pod_to_l2s(l1s_pods, delta)
                
                for pod in pods_to_l2_sleep:
                    future = executor.submit(self.set_pod_state_to_rln, service, pod)
                    ret_futures.append(future)
                
                l2s_pods += pods_to_l2_sleep 
                
            # Warm-up 
            warmup = False 
            if warm == 0 and len(initializing_pods) > 0 and time.time() - self.prev_warmup_ts > 15:
                warmup = True 
                pod = initializing_pods.pop(0)
                future = executor.submit(self.set_pod_state_to_rr, service, pod)
                ret_futures.append(future)
                
                running_pods += [pod] 
            
            for future in as_completed(ret_futures):
                future.result()
                
            if warm > 0 or warmup:
                self.prev_warmup_ts = time.time()
         
    def sync_replicas(self):  
        __l = self.__logger.getChild('MainSync')
        __l.info('Running ...')
        
        __sync_start = time.time()      
        ret_futures = list()
        with futures.ThreadPoolExecutor() as executor:
            for service, replicas_conf in self.replicas.items():
                ret_futures.append(executor.submit(self.__do_sync_replicas, service, replicas_conf))
        
        for future in as_completed(ret_futures):
            future.result()
        
        __l.info(f'use {time.time() - __sync_start} seconds.')
