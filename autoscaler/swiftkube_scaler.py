import time 
import os 
import math 
import json 
import copy 
import threading 
import logging 
import pickle
import traceback 
from concurrent import futures
from concurrent.futures import as_completed
from typing import List, Dict, Tuple 
from logging import Logger

from kafka import KafkaConsumer
import matplotlib.pyplot as plt 
from kubernetes.client import V1Pod
import numpy as np 
import pandas as pd 
import cvxpy as cp
import statsmodels.api as sm

from . import Scaler
from .ts_predictor.enbpi import EnbpiPredictor

# Swiftkube labels 
SWIFTKUBE_STATE_LABEL = 'swiftkube.io/state'

SWIFTKUBE_STATE_PRE_RUNNING = 'pre-running'
SWIFTKUBE_STATE_RUNNING = 'running'
SWIFTKUBE_STATE_PRE_SLEEPING_LEVEL1 = 'pre-sleeping-level1'
SWIFTKUBE_STATE_SLEEPING_LEVEL1 = 'sleeping-level1'
SWIFTKUBE_STATE_SLEEPING_LEVEL2 = 'sleeping-level2'

INIT_LABELS = {
    SWIFTKUBE_STATE_LABEL: SWIFTKUBE_STATE_RUNNING
}

OUTPUT = False 

class PandasDataset(object):
    def __init__(self, timestamp_col: str,
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
        #retval = np.mean(self.__data[col].values)
        # TODO 
        #retval += np.std(self.__data[col].values)
        self.__lock.release()
        return retval 
            

"""
RT
RT-L1
RT-L1-L2

Controller Return Value:
{
    "service_name1": replicas_num1,
    "service_name2": replicas_num2,
    ...
}

"""
class SwiftKubeScaler(Scaler):
    def __init__(self, cfg, logger: Logger):
        super().__init__(cfg, logger)
        
        self.__cfg = cfg 
        self.__logger = logger.getChild('Main')
        
        self.__kafka_consumer = None 
        self.__kafka_data: Dict[str, Dict[str, PandasDataset]] = dict()
        
        self.min_cpu_limit = cfg.scaler.swiftkube_scaler.min_cpu_limit
        self.strategy = cfg.scaler.swiftkube_scaler.strategy
        
        self.__pods_cache: Dict[str, List[V1Pod]] = dict()
        self.__pods_cache_lock = threading.Lock()
        
        self.last_st_controller_ts = None 
        self.last_lt_controller_ts = None 
        
        self.__service_endpoints_map: Dict[str, List[str]] = dict()
        self.__services = self.get_all_services_from_cfg()
        
        self.__st_predictors: Dict[str, Dict[str, EnbpiPredictor]] = dict()
        
        self.replicas: Dict[str, Dict[str, int]] = dict()
        
        self.__rt_l1_prev_result = None 
        self.__rt_l2_prev_result = None 
        self.__rt_l1_l2_prev_result = None 
        
        self.__lt_pred_result = None 
        
        self.__first_sync = True 
        self.__sync_replicas_future = None 
        self.__sync_executor = futures.ThreadPoolExecutor()
        
        # Logging 
        logfile_path = 'autoscaler/logs/'
        formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] - %(lineno)s: %(message)s')
        self.__operation_logfile = logging.FileHandler(logfile_path + '/swiftkube_operation.log')
        self.__operation_logfile.setFormatter(formatter)
        self.__rtc_logfile = logging.FileHandler(logfile_path + '/swiftkube_rt.log')
        self.__rtc_logfile.setFormatter(formatter)
        self.__stc_logfile = logging.FileHandler(logfile_path + '/swiftkube_st.log')
        self.__stc_logfile.setFormatter(formatter)
        self.__cl_logfile = logging.FileHandler(logfile_path + '/swiftkube_control_loop.log')
        self.__cl_logfile.setFormatter(formatter)
        
        # Init replicas and kafka data 
        for service in self.__services:
            
            init_running = 3
            init_replicas = self.get_service_max_replicas_from_cfg(service)
            init_l1s = init_replicas - init_running
            init_l2s = 0
            
            self.replicas[service] = dict(
                replicas=init_replicas,
                running=init_running,
                l1_sleep=init_l1s,
                l2_sleep=init_l2s
            )
            
            endpoints = self.get_service_endpoints(service)
            self.__service_endpoints_map[service] = endpoints
            
            for endpoint in endpoints:
                if service not in self.__kafka_data:
                    self.__kafka_data[service] = dict()
                if endpoint not in self.__kafka_data[service]:
                    self.__kafka_data[service][endpoint] = PandasDataset(
                        'timestamp',
                        10
                    )
        
        # Init EnbpiPredictor 
        for service, endpoints in self.__service_endpoints_map.items():
            for endpoint in endpoints:
                if service not in self.__st_predictors:
                    self.__st_predictors[service] = dict()
                if endpoint not in self.__st_predictors[service]:
                    __l = self.__logger.getChild(f'EnbPI-{service}-{endpoint}')
                    __enbpi_logfile = logging.FileHandler(logfile_path + f'/swiftkube_enbpi-{service}-{endpoint.replace("/", "_")}.log')
                    __enbpi_logfile.setFormatter(formatter)
                    __l.addHandler(__enbpi_logfile)
                    self.__st_predictors[service][endpoint] = \
                        EnbpiPredictor(
                            self.__cfg, 
                            __l, 
                            service,
                            endpoint,
                            '(span_count * (rt_mean / 1000))',
                            agg_function='mean'
                        )
    
    def kafka_consumer(self):
        self.__kafka_consumer = KafkaConsumer(
            self.__cfg.scaler.swiftkube_scaler.kafka.topic,
            bootstrap_servers=self.__cfg.scaler.swiftkube_scaler.kafka.bootstrap_servers 
        )
        
        def __append_data(msg):
            __l = self.__logger.getChild('KafkaAppender')
            value = json.loads(msg.value.decode()) 
            service_name = value['metadata']['serviceName']
            endpoint_name = value['metadata']['endpointName']
            data = {
                'concurrency': [value['concurrency']],
                'timestamp': [int(value['windowEndUnixTimestamp'] / 1000)]
            }
            __append_start = time.time()
            
            self.__kafka_data[service_name][endpoint_name].append(data)
            __l.debug(f'append use {time.time() - __append_start}s')
        
        with futures.ThreadPoolExecutor(max_workers=40) as executor:
            for msg in self.__kafka_consumer:
                executor.submit(__append_data, msg)
        
    def pre_start(self):
        self.__logger.info('SwiftKube Scaler preStart ...')
        self.__lt_logger = self.__logger.getChild('LongTermPred')
        
        output_path = 'autoscaler/swiftkube_data/output/'
        
        if self.strategy == 'rt-l1-l2' or self.strategy == 'rt-l2':
            data_path = 'autoscaler/swiftkube_data/lt_results.pkl'
            
            if os.path.exists(data_path):  # Already trained 
                self.__lt_logger.info('already trained.')
                with open(data_path, 'rb') as data_file:
                    self.__lt_pred_result = pickle.load(data_file) 
            else:   
                # Load training dataset 
                self.__logger.info('loading training dataset ...')
                raw_data_file = "/home/postgres/default/span_stats.csv"
                df = pd.read_csv(raw_data_file) 
                raw_service_datas: Dict[str, np.ndarray] = \
                    load_concurrency_dataset(df, self.__services) 
                smooth_datas: Dict[str, np.ndarray] = dict()
                
                # Smooth 
                self.__logger.info('smoothing training dataset ...')
                with futures.ThreadPoolExecutor(max_workers=80) as executor:
                    ret_futures = list()
                    
                    def __smooth(service, data):
                        data = pd.Series(data)
                        roller = data.rolling(window=60 * 10, center=True)
                        data = roller.apply(lambda x: x.mean() + 3 * x.std()).bfill().ffill()
                        return service, data.values 
                    
                    for key, array in raw_service_datas.items():
                        future = executor.submit(__smooth, key, array)
                        ret_futures.append(future)
                        
                    for future in as_completed(ret_futures):
                        service, retval = future.result()
                        smooth_datas[service] = retval 
                
                # load test dataset 
                self.__logger.info('loading test dataset ...')
                real_data_file = "/home/postgres/swiftkube-rt/span_stats.csv"
                df = pd.read_csv(real_data_file) 
                real_service_datas: Dict[str, np.ndarray] = \
                    load_concurrency_dataset(df, self.__services) 
                    
                # Plot traning datas 
                for service in self.__services:
                    ____data = raw_service_datas[service]
                    ____data2 = smooth_datas[service]
                    x = [i for i in range(len(____data))]
                    plt.plot(x, ____data, label=service + ":real")
                    plt.plot(x, ____data2, label=service + ":smooth")
                    plt.savefig(output_path + f'raw_concurrency/{service}.png')
                    plt.clf() 
                
                # Plot test datas 
                for service in self.__services:
                    ____data = real_service_datas[service]
                    x = [i for i in range(len(____data))]
                    plt.plot(x, ____data, label=service)
                    plt.savefig(output_path + f'real_concurrency/{service}.png')
                    plt.clf() 
                # Start training LTTF models 
                ret_futures = list()
                results = list()
                
                t0 = time.time()
                with futures.ThreadPoolExecutor() as executor:
                    for service in self.__services:
                        # Load dataset 
                        self.__lt_logger.info(f'Load dataset of {service}')
                        #service_history_data: np.ndarray = \
                        #    raw_service_datas[service]
                        service_history_data: np.ndarray = \
                            smooth_datas[service]
                        
                        # Training and predict 
                        long_term_pred = LongTermPred(self.__cfg, self.__logger, service_history_data)
                        future = executor.submit(long_term_pred.predict, service)
                        
                        ret_futures.append(future)

                    self.__lt_logger.info('Training and predicting ...')
                    
                    # Wait 
                    for future in as_completed(ret_futures):
                        results.append(future.result())
                    
                self.__lt_pred_result = results
                
                self.__logger.info(f'cost time: {time.time()-t0}')
                self.__logger.info(self.__lt_pred_result)
                
                # Plot LTTF predicted datas 
                for result in self.__lt_pred_result:
                    ____data = result['predict_result']
                    ____true_data = real_service_datas[result["service_name"]]
                    length = min(len(____data), len(____true_data))
                    x = [i for i in range(length)]
                    plt.plot(x, ____true_data[:length], label='true')
                    plt.plot(x, ____data[:length], label='predicted')
                    plt.legend()
                    plt.savefig(output_path + f'pred_concurrency/{result["service_name"]}.png')
                    plt.clf() 

                with open(data_path, 'wb') as file:
                    pickle.dump(results, file) 
                # Training done 
        
    def start(self):
        self.__logger.info('SwiftKube Scaler start ...')
        __cl_logger = self.__logger.getChild('ControlLoop')
        __cl_logger.addHandler(self.__cl_logfile)
        
        # Start kafka consumer 
        threading.Thread(target=self.kafka_consumer, daemon=True).start()
        
        wait_sec = 180
        self.__logger.info(f'Wait for {wait_sec} seconds.')
        time.sleep(wait_sec) 
        
        # Sync replcas 
        self.sync_replicas()
        
        # Init ST predictors 
        self.__logger.info('initializing ST predictors ...')
        with futures.ThreadPoolExecutor(max_workers=40) as executor:
            ret_futures = list()
            for _, endpoints in self.__st_predictors.items():
                for _, predictor in endpoints.items():
                    future = executor.submit(predictor.init)
                    ret_futures.append(future)
            # Wait 
            for future in as_completed(ret_futures):
                future.result()
            self.last_st_controller_ts = time.time()
        self.__logger.info('initialize ST predictors done')

        self.locust_start_time = time.time()
        while True:
            control_loop_start = time.time()
            
            #================== SwiftKube-SG-L1 (rt-l2-v2) or SwiftKube-SGC-L1 (rt-l2) =============#
            if self.strategy == 'rt-l2' or self.strategy == 'rt-l2-v2':
                
                # Run controllers 
                __run_controller_start = time.time()
                with futures.ThreadPoolExecutor(max_workers=2) as executor:
                    st_future = executor.submit(self.st_controller)
                    if self.strategy == 'rt-l2':
                        lt_future = executor.submit(self.lt_controller)
                    
                    rt_controller_result = self.rt_controller()
                    self.sync_running_replicas(rt_controller_result)
                    
                    while True:
                        try:
                            st_controller_result = st_future.result(0.9)
                            break 
                        except Exception as e:
                            __cl_logger.info(f'ST controller exception {traceback.format_exc()}')
                            rt_result = self.rt_controller()
                            self.sync_running_replicas(rt_result)
                    
                    rt_controller_result = self.rt_controller()
                    
                    if self.strategy == 'rt-l2':
                        lt_controller_result = lt_future.result()
                __run_controller_time = time.time() - __run_controller_start
                __cl_logger.info(f'Run controllers use {__run_controller_time}s')
                
                result = dict()
                
                # For each service 
                for service in self.__services:
                    result[service] = dict()
                    
                    rt_ret = rt_controller_result.get(service)
                    st_ret = st_controller_result.get(service)
                    if self.strategy == 'rt-l2':
                        lt_ret = lt_controller_result.get(service)
                    
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    rt_ret = min(rt_ret, max_replicas)
                    st_ret = min(st_ret, max_replicas)
                    if self.strategy == 'rt-l2':
                        lt_ret = min(lt_ret, max_replicas)
                    
                    if self.__rt_l2_prev_result is not None:
                        prev_replicas_up_ts = self.__rt_l2_prev_result[service]['__replicas_up_ts__']
                        prev_replicas_down_ts = self.__rt_l2_prev_result[service]['__replicas_down_ts__']
                        prev_replicas = self.__rt_l2_prev_result[service]['replicas']
                    else:
                        prev_replicas_up_ts = 0
                        prev_replicas_down_ts = 0
                        prev_replicas = max_replicas
                    
                    # Replicas     
                    up_ts_delta = time.time() - prev_replicas_up_ts
                    down_ts_delta = time.time() - prev_replicas_down_ts
                    if self.strategy == 'rt-l2':
                        replicas = max(st_ret, lt_ret)
                    else:
                        replicas = st_ret 
                    replicas = max(replicas, 1)
                    #replicas = max(prev_replicas - 3, replicas)
                    if replicas < prev_replicas:
                        if down_ts_delta <= 60:
                            replicas = prev_replicas 
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                        else:
                            result[service]['__replicas_down_ts__'] = time.time()
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    elif replicas > prev_replicas:
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
                    self.replicas[service] = conf 
            
            #================== SwiftKube-SGC-L2 =========================#
            if self.strategy == 'rt-l1-l2':
                
                # Run controllers 
                __run_controller_start = time.time()
                with futures.ThreadPoolExecutor(max_workers=2) as executor:
                    rt_future = executor.submit(self.rt_controller)
                    st_future = executor.submit(self.st_controller)
                    lt_future = executor.submit(self.lt_controller)
                    
                    rt_controller_result = rt_future.result()
                    # Sync running replicas 
                    self.sync_running_replicas(rt_controller_result)
                    
                    st_controller_result = st_future.result()
                    lt_controller_result = lt_future.result()
                __run_controller_time = time.time() - __run_controller_start
                __cl_logger.info(f'Run controllers use {__run_controller_time}s')
                
                result = dict()
                
                # For each service 
                for service in self.__services:
                    result[service] = dict()
                    
                    rt_ret = rt_controller_result.get(service)
                    st_ret = st_controller_result.get(service)
                    lt_ret = lt_controller_result.get(service)
                    
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    rt_ret = min(rt_ret, max_replicas)
                    st_ret = min(st_ret, max_replicas)
                    lt_ret = min(lt_ret, max_replicas)
                    
                    if self.__rt_l1_l2_prev_result is not None:
                        prev_replicas_up_ts = self.__rt_l1_l2_prev_result[service]['__replicas_up_ts__']
                        prev_replicas_down_ts = self.__rt_l1_l2_prev_result[service]['__replicas_down_ts__']
                        prev_replicas = self.__rt_l1_l2_prev_result[service]['replicas']
                        prev_s1 = self.__rt_l1_l2_prev_result[service]['l1_sleep']
                        prev_running = self.__rt_l1_l2_prev_result[service]['running']
                    else:
                        prev_replicas_up_ts = 0
                        prev_replicas_down_ts = 0
                        prev_replicas = max_replicas
                        prev_s1 = 0
                        prev_running = 0
                    
                    # Replicas     
                    up_ts_delta = time.time() - prev_replicas_up_ts
                    down_ts_delta = time.time() - prev_replicas_down_ts
                    replicas = max(st_ret, lt_ret)
                    replicas = max(prev_replicas - 1, replicas)
                    if replicas < prev_replicas:
                        if down_ts_delta <= 60:
                            replicas = prev_replicas 
                            result[service]['__replicas_down_ts__'] = prev_replicas_down_ts
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                        else:
                            result[service]['__replicas_down_ts__'] = time.time()
                            result[service]['__replicas_up_ts__'] = prev_replicas_up_ts
                    elif replicas > prev_replicas:
                        if up_ts_delta < 60:
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
                    l1_sleep_replicas = max(min(st_ret, lt_ret) - rt_ret, 0)
                    if l1_sleep_replicas == 0:
                        l1_sleep_replicas = max(0, prev_running + prev_s1 - running_replicas)
                        l1_sleep_replicas = min(replicas - running_replicas, l1_sleep_replicas)
                    result[service]['l1_sleep'] = l1_sleep_replicas
                    
                    # Level2 Sleep 
                    l2_sleep_replicsa = max(0, replicas - running_replicas - l1_sleep_replicas)
                    result[service]['l2_sleep'] = l2_sleep_replicsa
                
                self.__rt_l1_l2_prev_result = result 
                
                for service, conf in result.items():
                    self.replicas[service] = conf 
            
            #================== SwiftKube-SG-L2 ==========================#
            elif self.strategy == 'rt-l1':
                
                # Run controllers 
                __run_controller_start = time.time()
                with futures.ThreadPoolExecutor(max_workers=2) as executor:
                    st_future = executor.submit(self.st_controller)
                    
                    #rt_controller_result = self.rt_controller()
                    #self.sync_running_replicas(rt_controller_result)
                    st_controller_result = st_future.result()
                    """
                    while True:
                        try:
                            st_controller_result = st_future.result(0.9)
                            break 
                        except:
                            __cl_logger.info(f'ST controller exception {traceback.format_exc()}')
                            rt_result = self.rt_controller()
                            self.sync_running_replicas(rt_result)
                    """
                    rt_controller_result = self.rt_controller()
                    #self.sync_running_replicas(rt_controller_result)
                    
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
                    rt_ret = min(rt_ret, max_replicas)
                    if self.__rt_l1_prev_result is not None:
                        prev_running = self.__rt_l1_prev_result[service]['running']
                        rt_ret = min(rt_ret, prev_running + 1)
                        
                    if st_ret >= 0:
                        st_ret = min(st_ret + 1, max_replicas)
                    
                    if st_ret >= 0:
                        l1_sleep_replicas = max(st_ret - rt_ret, 0)
                    else:
                        l1_sleep_replicas = -1
                    
                    result[service] = dict(
                        replicas=max_replicas,
                        running=rt_ret,
                        l1_sleep=l1_sleep_replicas,
                        l2_sleep=max_replicas - rt_ret - l1_sleep_replicas
                    )
                
                # Fix result (Running pods should not be directly transformed to s2)
                if self.__rt_l1_prev_result is not None:
                    for service in self.__services:  
                        prev_l1_sleep = self.__rt_l1_prev_result[service]['l1_sleep']
                        tgt_l1_sleep = result[service]['l1_sleep']
                        
                        prev_running = self.__rt_l1_prev_result[service]['running']
                        tgt_running = result[service]['running']
                        
                        if tgt_l1_sleep == 0:
                            if prev_l1_sleep + prev_running > tgt_running:
                                __delta = prev_l1_sleep + prev_running - tgt_running
                                result[service]['l2_sleep'] = result[service]['l2_sleep'] - __delta
                                result[service]['l1_sleep'] = result[service]['l1_sleep'] + __delta 
                        elif tgt_l1_sleep == -1:
                            if prev_l1_sleep + prev_running > tgt_running:
                                result[service]['l1_sleep'] = prev_l1_sleep + prev_running - tgt_running
                                result[service]['l2_sleep'] = self.get_service_max_replicas_from_cfg(service) - \
                                    result[service]['l1_sleep'] - result[service]['running']
                            else:
                                result[service]['l1_sleep'] = 0
                                result[service]['l2_sleep'] = self.get_service_max_replicas_from_cfg(service) - \
                                    result[service]['l1_sleep'] - result[service]['running']
                else:
                    for service in self.__services:
                        tgt_l1_sleep = result[service]['l1_sleep']
                        if tgt_l1_sleep == -1:
                            result[service]['l1_sleep'] = self.get_service_max_replicas_from_cfg(service) - \
                                result[service]['running']
                            result[service]['l2_sleep'] = 0
                
                # Refresh prev result 
                self.__rt_l1_prev_result = result 
                
                # Update replicas 
                for service, conf in result.items():
                    self.replicas[service] = conf 
                    running = conf['running']
                    l1s = conf['l1_sleep']
                    l2s = conf['l2_sleep']
                    self.__logger.info(f'{service} running: {running} l1_sleep: {l1s} l2_sleep: {l2s}')
            
            #================== SwiftKube-S-L1 ==========================# 
            elif self.strategy == 'rt':
                # Run RT-controller 
                rt_controller_result = self.rt_controller()
                
                result = dict()
                
                # For each service ...
                for service in self.__services:
                    rt_ret = rt_controller_result.get(service)
                    max_replicas = self.get_service_max_replicas_from_cfg(service)
                    rt_ret = min(rt_ret, max_replicas)
                    
                    result[service] = dict(
                        replicas=max_replicas,
                        running=rt_ret,
                        l1_sleep=max_replicas - rt_ret,
                        l2_sleep=0
                    ) 
                
                # Update replicas 
                for service, conf in result.items():
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
        __rt_logger.addHandler(self.__rtc_logfile)
        __rt_logger.debug('Running ...')
        
        def __get_concurrency(service_name) -> Tuple[str, int]:
            retval = 0
            for endpoint in self.__service_endpoints_map[service_name]:
                retval += self.__kafka_data[service_name][endpoint].get_mean_value('concurrency')
            return service_name, retval 
        
        retval = dict()
        ret_futures = list()
        
        with futures.ThreadPoolExecutor() as executor:
            for service in self.__services:
                future = executor.submit(__get_concurrency, service) 
                ret_futures.append(future)
                
            for future in as_completed(ret_futures):
                result = future.result()
                retval[result[0]] = result[1]
        
        for service_name, concurrent in retval.items():
            max_worker = self.get_service_max_worker_from_cfg(service_name)
            
            # 100(100%) or 50(50%) or else 
            target = self.get_service_worker_target_utilization_from_cfg(service_name)
            threshold = max_worker * (target / 100)
            
            retval[service_name] = math.ceil(concurrent / threshold)
        
        __rt_logger.debug(f'use {time.time() - __rt_start}s.')
        return retval 
    
    def st_controller(self) -> Dict[str, int]:
        retval = dict()
        __l = self.__logger.getChild('STController')
        __l.addHandler(self.__stc_logfile)
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
            
            # 100(100%) or 50(50%) or else 
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
        
        # TODO not consider possible ceil
        idx = int(__lt_start - self.locust_start_time) // 120 * 120

        for dic in self.__lt_pred_result:
            service = dic['service_name']
            concurrency = dic['predict_result'][idx]
            
            max_worker = self.get_service_max_worker_from_cfg(service)
            
            # 100(100%) or 50(50%) or else 
            target = self.get_service_worker_target_utilization_from_cfg(service)
            threshold = max_worker * (target / 100)
            
            retval[service] = math.ceil(concurrency / threshold)
        return retval
    
    def __refit_st_predictors(self):
        for _, endpoints in self.__st_predictors.items():
            for _, predictor in endpoints.items():
                predictor.refit()
    
    def __st_predict_concurrency(self, service_name, endpoint_name, fit=False):
        assert service_name is not None
        assert endpoint_name is not None 
        
        result = dict(
            service_name=service_name,
            endpoint_name=endpoint_name
        )
        
        #self.__logger.info(f'predicting concurrency of {service_name} {endpoint_name}')
        retval = self.__st_predictors[service_name][endpoint_name].predict()
        result['predict_result'] = retval 
        
        return result 
            
    def st_predict_concurrency(self, fit=False):
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
    
    def __refresh_pod(self, service_name, pod):
        self.__pods_cache_lock.acquire()
        pods = self.__pods_cache[service_name]
        for i in range(len(pods)):
            __pod = pods[i]
            if __pod.metadata.name == pod.metadata.name and \
                __pod.metadata.namespace == pod.metadata.namespace:
                pods[i] = pod 
                break 
        self.__pods_cache_lock.release()
    
    def set_pod_to_running(self, service_name: str, pod: V1Pod):
        __l = self.__logger.getChild('Operation')
        __l.addHandler(self.__operation_logfile)
        
        tgt_label = SWIFTKUBE_STATE_RUNNING
        # TODO 
        #if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING_LEVEL2:
        #    tgt_label = SWIFTKUBE_STATE_PRE_RUNNING
        
        try:
            pod = self.patch_k8s_pod(
                pod.metadata.name, pod.metadata.namespace,
                body={
                    'spec': {
                        'containers': copy.deepcopy(self.get_resources_config_from_cfg(service_name))
                    }
                }
            )
        except Exception as e:
            __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to running stats failed: {e}.')
        
        try:
            pod = self.patch_k8s_pod(
                pod.metadata.name, pod.metadata.namespace,
                body={
                    'metadata': {
                        'labels': {
                            SWIFTKUBE_STATE_LABEL: tgt_label
                        },
                        'annotations': {
                            'controller.kubernetes.io/pod-deletion-cost': "10000"
                        }
                    },
                }
            )
        except Exception as e:
            __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to running stats failed: {e}.')
        
        if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_PRE_RUNNING:
            pod.metadata.labels[SWIFTKUBE_STATE_LABEL] = SWIFTKUBE_STATE_RUNNING
        
        self.__refresh_pod(service_name, pod)
        
        __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to running stats.')
    
    def set_pod_to_l1_sleep(self, service_name, pod: V1Pod):
        __l = self.__logger.getChild('Operation')
        __l.addHandler(self.__operation_logfile)
        
        tgt_label = SWIFTKUBE_STATE_SLEEPING_LEVEL1
        # TODO 
        #if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING_LEVEL2:
        #    tgt_label = SWIFTKUBE_STATE_PRE_SLEEPING_LEVEL1
        
        resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
        for config in resources_config:
            if 'limits' in config['resources']:
                if 'cpu' in config['resources']['limits']:
                    config['resources']['limits']['cpu'] = '3000m' # TODO 
            if 'requests' in config['resources']:
                if 'cpu' in config['resources']['requests']:
                    config['resources']['requests']['cpu'] = '10m' 
        
        try:
            pod = self.patch_k8s_pod(
                pod.metadata.name, pod.metadata.namespace,
                body={
                    'metadata': {
                        'labels': {
                            SWIFTKUBE_STATE_LABEL: tgt_label
                        },
                        'annotations': {
                            'controller.kubernetes.io/pod-deletion-cost': "5000"
                        }
                    },
                    'spec': {
                        'containers': resources_config
                    }
                }
            )
        except Exception as e:
            __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to l1_sleep stats failed: {e}.')
        
        if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_PRE_SLEEPING_LEVEL1:
            pod.metadata.labels[SWIFTKUBE_STATE_LABEL] = SWIFTKUBE_STATE_SLEEPING_LEVEL1
        
        self.__refresh_pod(service_name, pod)
        
        __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to l1_sleep stats.')
    
    def set_pod_to_l2_sleep(self, service_name, pod: V1Pod):
        __l = self.__logger.getChild('Operation')
        __l.addHandler(self.__operation_logfile)
        
        resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
        for config in resources_config:
            if 'limits' in config['resources']:
                if 'cpu' in config['resources']['limits']:
                    pass  # TODO 
                if 'memory' in config['resources']['limits']:
                    pass  # TODO   # TODO 
            if 'requests' in config['resources']:
                if 'cpu' in config['resources']['requests']:
                    config['resources']['requests']['cpu'] = '10m' 
                if 'memory' in config['resources']['requests']:
                    config['resources']['requests']['memory'] = '100Mi'  # TODO 
        try:
            pod = self.patch_k8s_pod(
                pod.metadata.name, pod.metadata.namespace,
                body={
                    'metadata': {
                        'labels': {
                            SWIFTKUBE_STATE_LABEL: SWIFTKUBE_STATE_SLEEPING_LEVEL2
                        },
                        'annotations': {
                            'controller.kubernetes.io/pod-deletion-cost': "0"
                        }
                    },
                    'spec': {
                        'containers': resources_config
                    }
                }
            )
        except Exception as e:
            __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to l2_sleep stats failed: {e}.')
        
        self.__refresh_pod(service_name, pod)
        
        __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) to l2_sleep stats.')
        
    def choose_pod_to_l2_sleep_from_l1(self,
                                       pods: List[V1Pod],
                                       delta: int) -> List[V1Pod]:
        ret_pods = list()
        if len(pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < delta and len(pods) > 0:
            ret_pods.append(pods.pop(0))
            
        return ret_pods 
        
    def choose_pod_to_l1_sleep_from_l2(self,
                                       s2_pods: List[V1Pod],
                                       delta: int) -> List[V1Pod]:
        ret_pods = list()
        if len(s2_pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < delta and len(s2_pods) > 0:
            ret_pods.append(s2_pods.pop(0))
            
        return ret_pods 
    
    def choose_pod_to_l1_sleep_from_run(self, 
                                        running_pods: List[V1Pod], 
                                        delta: int) -> List[V1Pod]:
        ret_pods = list()
        if len(running_pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < delta and len(running_pods) > 0:
            ret_pods.append(running_pods.pop(0))
            
        return ret_pods 
    
    def choose_pod_to_run(self,  
                          s1_pods: List[V1Pod], 
                          s2_pods: List[V1Pod],
                          delta: int) -> List[V1Pod]:
        ret_pods = list()
        
        while len(ret_pods) < delta and len(s1_pods) > 0:
            ret_pods.append(s1_pods.pop(0))
        if len(ret_pods) == delta:
            return ret_pods 
        else:
            while len(ret_pods) < delta and len(s2_pods) > 0:
                ret_pods.append(s2_pods.pop(0))
                
        return ret_pods 
    
    def __refresh_pods_cache(self, service, dep_name, namespace, dep_obj=None):
        __l = self.__logger.getChild(f'RefreshCache-{service}')
        __start = time.time()
        pods: List[V1Pod] = self.list_pods_of_dep(dep_name, namespace, dep_obj).items
        for pod in pods:
            if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_PRE_RUNNING:
                pod.metadata.labels[SWIFTKUBE_STATE_LABEL] = SWIFTKUBE_STATE_RUNNING
            if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_PRE_SLEEPING_LEVEL1:
                pod.metadata.labels[SWIFTKUBE_STATE_LABEL] = SWIFTKUBE_STATE_SLEEPING_LEVEL1
        pods_filted = list()
        for pod in pods:
            if pod.status.phase == 'Running':
                pods_filted.append(pod)
        
        self.__pods_cache_lock.acquire()
        self.__pods_cache[service] = pods_filted
        self.__pods_cache_lock.release()
        
        __l.debug(f'Refresh cache use {time.time() - __start}s.')
    
    def __do_sync_replicas(self, service, replicas_conf):
        replicas = replicas_conf.get('replicas') 
        running_replicas = replicas_conf.get('running')
        l1_sleep_replicas = replicas_conf.get('l1_sleep')
        l2_sleep_replicas = replicas_conf.get('l2_sleep')
        
        assert running_replicas + l1_sleep_replicas + l2_sleep_replicas == replicas 
        
        dep_name = self.get_k8s_dep_name_from_cfg(service)
        namespace = self.get_k8s_namespace_from_cfg()
         
        # Set replicas field of Deployment 
        need_refresh_cache = False 
        dep_obj = self.get_k8s_deployment(dep_name, namespace)
        
        ops = 'none'
        if dep_obj.spec.replicas > replicas:
            ops = 'down'
        elif dep_obj.spec.replicas < replicas:
            ops = 'up'
        
        if ops != 'none': 
            need_refresh_cache = True 
            dep_obj = self.set_k8s_deployment_replicas(dep_name, namespace, replicas)
        
        if self.__first_sync:
            need_refresh_cache = True 
        
        # TODO Set every pod to Running if scale-in 
        if ops == 'down':
            pods = self.__pods_cache[service]
            with futures.ThreadPoolExecutor(max_workers=200) as executor:
                ret_futures = list()
                for pod in pods:
                    if pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_RUNNING:
                        future = executor.submit(self.set_pod_to_running, service, pod)
                        ret_futures.append(future)
                for future in as_completed(ret_futures):
                    future.result()
                    
        # Wait
        if ops == 'down':
            nr_pods = len(self.list_pods_of_dep(dep_name, namespace, dep_obj).items)
            print(f'nr_pods: {nr_pods} replicas: {replicas}')
            while nr_pods != replicas:
                nr_pods = len(self.list_pods_of_dep(dep_name, namespace, dep_obj).items)
                print(f'nr_pods: {nr_pods} replicas: {replicas}')
        if ops == 'up':
            ready_replicas = dep_obj.status.ready_replicas
            print(f'ready: {ready_replicas} replicas: {replicas}')
            while ready_replicas != replicas:
                rt_result = self.rt_controller()
                self.sync_running_replicas(rt_result)
                time.sleep(0.5)
                ready_replicas = self.get_k8s_deployment_ready_replicas(dep_name, namespace)
                print(f'ready: {ready_replicas} replicas: {replicas}')
        
        # Refresh Pods cache 
        if need_refresh_cache or (service not in self.__pods_cache):
            self.__refresh_pods_cache(service, dep_name, namespace, dep_obj)
        
        pods = self.__pods_cache[service]
            
        running_pods, s1_pods, s2_pods = list(), list(), list()
        for pod in pods:
            if SWIFTKUBE_STATE_LABEL not in pod.metadata.labels:
                raise Exception(f'Pod(name={pod.metadata.name} namespace={pod.metadata.namespace}) do not has swiftkube label.')
            # Running pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_RUNNING:
                running_pods.append(pod)
            # Sleeping (Level1) pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING_LEVEL1:
                s1_pods.append(pod)
            # Sleeping (Level2) pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING_LEVEL2:
                s2_pods.append(pod)
        
        ret_futures = list()
        with futures.ThreadPoolExecutor(max_workers=200) as executor:
            
            # Need more running pods 
            if len(running_pods) < running_replicas:
                delta_to_run = running_replicas - len(running_pods)
                pods_to_run = self.choose_pod_to_run(s1_pods, s2_pods, delta_to_run)

                for pod in pods_to_run:
                    future = executor.submit(self.set_pod_to_running, service, pod)
                    ret_futures.append(future)
                    
                running_pods += pods_to_run 
            
            # Need less running pods 
            elif len(running_pods) > running_replicas:
                delta = len(running_pods) - running_replicas
                pods_to_l1_sleep = self.choose_pod_to_l1_sleep_from_run(running_pods, delta) 
                
                for pod in pods_to_l1_sleep:
                    future = executor.submit(self.set_pod_to_l1_sleep, service, pod)
                    ret_futures.append(future)
                    
                s1_pods += pods_to_l1_sleep 
            
            # Need more s1 pods 
            if len(s1_pods) < l1_sleep_replicas:
                delta = l1_sleep_replicas - len(s1_pods)
                pods_to_l1_sleep = self.choose_pod_to_l1_sleep_from_l2(s2_pods, delta)
                
                for pod in pods_to_l1_sleep:
                    future = executor.submit(self.set_pod_to_l1_sleep, service, pod)
                    ret_futures.append(future)
                
                s1_pods += pods_to_l1_sleep 
            
            # Need less s1 pods 
            elif len(s1_pods) > l1_sleep_replicas:
                delta = len(s1_pods) - l1_sleep_replicas
                pods_to_l2_sleep = self.choose_pod_to_l2_sleep_from_l1(s1_pods, delta)
                
                for pod in pods_to_l2_sleep:
                    future = executor.submit(self.set_pod_to_l2_sleep, service, pod)
                    ret_futures.append(future)
                
                s2_pods += pods_to_l2_sleep 
            
            for future in as_completed(ret_futures):
                future.result()
         
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
            
        self.__first_sync = False 
        
        __l.info(f'use {time.time() - __sync_start} seconds.')
        
    def __do_sync_running_replicas(self, service, replicas):
        running_replicas = replicas 
        
        if service not in self.__pods_cache:
            return
        pods = self.__pods_cache[service]
        
        running_pods, s1_pods, s2_pods = list(), list(), list()
        for pod in pods:
            if SWIFTKUBE_STATE_LABEL not in pod.metadata.labels:
                raise Exception(f'Pod(name={pod.metadata.name} namespace={pod.metadata.namespace}) do not has swiftkube label.')
            # Running pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_RUNNING:
                running_pods.append(pod)
            # Sleeping (Level1) pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING_LEVEL1:
                s1_pods.append(pod)
            # Sleeping (Level2) pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING_LEVEL2:
                s2_pods.append(pod)
        
        ret_futures = list()
        with futures.ThreadPoolExecutor(max_workers=200) as executor:
            # Need more running pods 
            if len(running_pods) < running_replicas:
                delta_to_run = running_replicas - len(running_pods)
                pods_to_run = self.choose_pod_to_run(s1_pods, s2_pods, delta_to_run)

                for pod in pods_to_run:
                    future = executor.submit(self.set_pod_to_running, service, pod)
                    ret_futures.append(future)
                    
                running_pods += pods_to_run 
            
            # Need less running pods 
            elif len(running_pods) > running_replicas:
                delta = len(running_pods) - running_replicas
                pods_to_l1_sleep = self.choose_pod_to_l1_sleep_from_run(running_pods, delta) 
                
                for pod in pods_to_l1_sleep:
                    future = executor.submit(self.set_pod_to_l1_sleep, service, pod)
                    ret_futures.append(future)
                    
                s1_pods += pods_to_l1_sleep 
         
    
    def sync_running_replicas(self, config: Dict):
        __l = self.__logger.getChild('RunningSync')
        __l.debug('Running ...')
        
        __sync_start = time.time()      
        ret_futures = list()
        with futures.ThreadPoolExecutor() as executor:
            for service, replicas in config.items():
                ret_futures.append(executor.submit(self.__do_sync_running_replicas, service, replicas))
        
        for future in as_completed(ret_futures):
            future.result()
        
        __l.debug(f'use {time.time() - __sync_start} seconds.')


class LongTermPred(object):
    
    def __init__(self, cfg, logger, service_history_data):
        self.__logger = logger.getChild('Main')
                
        self.data = service_history_data
        
        self.T = cfg.scaler.ahpa_scaler.T
        self.H = cfg.scaler.ahpa_scaler.H
        self.lambda_1 = cfg.scaler.ahpa_scaler.lambda_1
        self.lambda_2 = cfg.scaler.ahpa_scaler.lambda_2
        self.delta_d = cfg.scaler.ahpa_scaler.delta_d
        self.delta_i = cfg.scaler.ahpa_scaler.delta_i
        
        self.forecast_length = cfg.scaler.ahpa_scaler.forecast_length
        
        self.quantile = cfg.scaler.ahpa_scaler.quantile
        # self. = cfg.scaler.ahpa_scaler.
        self.trim_left = cfg.scaler.ahpa_scaler.trim_left
        self.trim_right = cfg.scaler.ahpa_scaler.trim_right
        
        self.guessd_delay = cfg.scaler.ahpa_scaler.guessd_delay
        self.throughput = cfg.scaler.ahpa_scaler.throughput


    
    def bilateral_filter(self, time_series, delta_d, delta_i, H):
        N = len(time_series)
        filtered_series = np.zeros(N)
        for t in range(N):
            weights = np.exp(-((np.arange(max(0, t-H), min(N, t+H+1)) - t) ** 2) / (2 * delta_d ** 2)) \
                    * np.exp(-((time_series[max(0, t-H):min(N, t+H+1)] - time_series[t]) ** 2) / (2 * delta_i ** 2))
            filtered_series[t] = np.sum(weights * time_series[max(0, t-H):min(N, t+H+1)]) / np.sum(weights)
        return filtered_series

    def robust_trend_extraction(self, time_series, lambda_1, lambda_2, T):
        N = len(time_series)
        X = np.arange(N)
        y = time_series

        # Trend variables
        L = cp.Variable(N)

        # Piecewise-linear trend fit
        fit = cp.abs(y - L)
        fit = cp.sum(fit)

        # Regularization terms
        reg1 = cp.norm(L[1:] - L[:-1], 1)
        reg2 = cp.norm(L[T:] - L[:-T], 1)

        # Objective
        objective = cp.Minimize(fit + lambda_1 * reg1 + lambda_2 * reg2)
        constraints = []

        # Problem
        prob = cp.Problem(objective, constraints)
        prob.solve(solver=cp.ECOS)

        return L.value

    def seasonal_adjustment(self, time_series, trend, T):
        adjusted_series = time_series - trend
        seasonal = np.zeros_like(time_series)
        for t in range(T):
            idx = np.arange(t, len(time_series), T)
            seasonal[idx] = np.mean(adjusted_series[idx])
        return seasonal  # This is the actual seasonal component


    def apply_robustSTL(self, time_series, delta_d, delta_i, H, lambda_1, lambda_2, T):
        # Bilateral filter to remove noise
        filtered_series = self.bilateral_filter(time_series, delta_d, delta_i, H)

        # Robust extraction of trend
        trend = self.robust_trend_extraction(filtered_series, lambda_1, lambda_2, T)

        # Seasonal adjustment to extract seasonal component
        seasonal = self.seasonal_adjustment(time_series, trend, T)

        # Calculation of remainder component
        remainder = time_series - trend - seasonal

        # print('trend', trend.shape, 'seasonal', seasonal.shape, 'remainder', remainder.shape)
        return trend, seasonal, remainder    
    
    def predict_without_shift(self, time_series):
        
        # RobustSTL decomposition
        trend, seasonal, remainder = self.apply_robustSTL(
            time_series, 
            T=self.T, 
            H=self.H, 
            lambda_1=self.lambda_1, 
            lambda_2=self.lambda_2, 
            delta_d=self.delta_d, 
            delta_i=self.delta_i
        )
            
        # Remainder prediction
        train_length = len(time_series)
        forecast_length = self.forecast_length
        ts = np.arange(1, train_length + 1)
        y = time_series
        X = sm.add_constant(ts)
        quantile = 0.1
        quantreg_model = sm.QuantReg(y, X).fit(q=quantile)
        quantile_forecast_time = np.arange(train_length + 1, train_length + forecast_length + 1)
        quantile_forecast_time = sm.add_constant(quantile_forecast_time)
        remainder_pred = quantreg_model.predict(quantile_forecast_time)

        forecast = seasonal[-forecast_length:] + trend[-forecast_length:] + remainder_pred[-forecast_length:]
        
        return forecast

    def forecast_shifting(self, 
                          probabilistic_forecast, 
                          guessed_delay, 
                          throughput, 
                          current_host_count):
        n = len(probabilistic_forecast)
        adjusted_forecast = probabilistic_forecast

        # 1. Adjust the forecast backwards to account for throughouput limitations
        for t in range(n - 2, -1, -1):
            adjusted_forecast[t] = max(adjusted_forecast[t], adjusted_forecast[t + 1] - throughput)

        # 2. Compute positive and negative adjustments
        delta = [adjusted_forecast[0] - current_host_count]
        # print(delta, current_host_count)
        for t in range(1, n):
            delta.append(adjusted_forecast[t] - adjusted_forecast[t - 1])

        requests, releases = [], []
        for d in delta:
            if d > 0:
                requests.append(d)
                releases.append(0)
            else:
                requests.append(0)
                releases.append(-d)

        # 3. Shift requests in the past according to the expected delay
        shifted_requests = [0] * n
        for t in range(n):
            shift_index = t - guessed_delay
            if shift_index >= 0:
                shifted_requests[shift_index] = requests[t]
                # # 避免冲突
                # releases[shift_index] = 0

        return shifted_requests, releases    
    
    def predict(self, service_name):
        # 0. Get data
        data = self.data
        
        # 1. Predict next 14400 seconds
        forecast = self.predict_without_shift(data)

        # 2. Time shifting
        guessd_delay = self.guessd_delay
        throughput = self.throughput
        current_host_count = forecast[0]
        shifted_requests, releases = self.forecast_shifting(forecast, 
                                                            guessd_delay, 
                                                            throughput, 
                                                            current_host_count)

        actions = []
        forecast_shifted = []
        for i in range(len(shifted_requests)):
            actions.append(shifted_requests[i] - releases[i])
            forecast_shifted.append(current_host_count + np.sum(actions[:i]))
        
        # 3. Trimming
        forecast_shifted = forecast_shifted[self.trim_left: -self.trim_right]
        
        result = dict(service_name=service_name)
        result['predict_result'] = forecast_shifted        
        
        print(f'{service_name} has finish')        
        return result
    
    
def load_concurrency_dataset(data: pd.DataFrame, services: List[str]):
    retval = dict()
    for service in services:
        __data = data[data.service_name == service].copy(deep=True)
        endpoints = np.unique(__data['endpoint_name'].values)
        
        __ep_datas = None 
        idx = 0
        for endpoint in endpoints:
            __ep_data = __data[__data.endpoint_name == endpoint].copy(deep=True)
            __ep_data[f'concurrency{idx}'] = (__ep_data['rt_mean'] / 1000) * __ep_data['span_count']
            __ep_data = __ep_data[['timestamp', f'concurrency{idx}']]
            if __ep_datas is None:
                __ep_datas = __ep_data
            else:
                __ep_datas = pd.merge(__ep_datas, __ep_data, on='timestamp', how='outer').fillna(0)
            idx += 1
        for idx in range(len(endpoints)):
            if idx == 0:
                __ep_datas['concurrency-sum'] = __ep_datas[f'concurrency{idx}'] 
            else:
                __ep_datas['concurrency-sum'] = __ep_datas['concurrency-sum'] + __ep_datas[f'concurrency{idx}']
        __ep_datas.sort_values('timestamp', inplace=True)
        retval[service] = __ep_datas['concurrency-sum'].values 
    
    return retval 
 