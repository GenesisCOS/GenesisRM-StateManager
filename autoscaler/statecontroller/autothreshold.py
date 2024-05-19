import json 
import time 
import math 
import os 
import random 
import requests 
import subprocess
import copy 
import pathlib 
import threading 
from concurrent import futures
from concurrent.futures import as_completed
from logging import Logger
from typing import Dict 

from kubernetes.client.rest import ApiException
import vowpalwabbit
from omegaconf import DictConfig
import numpy as np 
import pandas as pd

from . import Scaler
from ..util.locust import Locust  


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

APPMANAGER_HOST = 'localhost:10000'

ROOT_PATH = os.path.split(os.path.realpath(__file__))[0]

def swift_list_pods_of_dep(namespace, label, value):
    
    resp = requests.post(
        f'http://{APPMANAGER_HOST}/pods/lister/',
        json=dict(
            namespace=namespace,
            label=label,
            value=value
        ),
        headers={
            'Connection': 'close'
        }
    )
    return json.loads(resp.text)


class LocustSample(object):
    def __init__(self, stats_df: pd.DataFrame):
        self.__p99_rt = stats_df['p99_rt'].values[0]
        self.__p95_rt = stats_df['p95_rt'].values[0]
        self.__p50_rt = stats_df['p50_rt'].values[0]
        self.__mean_rt = stats_df['mean_rt'].values[0]
        self.__rps = stats_df['rps'].values[0] 
    
    def response_time(self, type: str):
        """Get response time 

        Args:
            type (str): mean, p99 or p95 
        """
        if type == 'mean':
            return self.__mean_rt 
        elif type == 'p99':
            return self.__p99_rt
        elif type == 'p95':
            return self.__p95_rt 
        elif type == 'p50':
            return self.__p50_rt
    
    def request_per_second(self) -> int:
        """Get RPS 
        """
        return self.__rps  


class AutoThreshold(Scaler):
    def __init__(self, cfg: DictConfig, data_dir, logger: Logger):
        super().__init__(cfg, logger)
        self.__cfg = cfg 
        self.__scaler_cfg = cfg.scaler.cb_scaler  
        self.__logger = logger
        self.__logger.info('AutoThreshold 初始化')
        self.__SLO = 1000
        self.__logger.info(f'SLO 为 {self.__SLO} 毫秒')
        self.__priv_data_path = data_dir
        #self.__workload = 'nasa_1day_6hour_min_700'
        self.__workload = 'const_1000'
        self.__locust = Locust(
            self.__cfg, f'csv-output-{int(time.time())}',
            self.__workload, self.__logger.getChild('Locust')
        ) 
        self.__explore_episode = 360 * 8  # 八个“NASA日”
        self.__svc_groups = self.__scaler_cfg.groups 
        self.__thresholds = list() 
        self.__cb_nr_action = 1
        self.__nr_group = len(self.__svc_groups)
        self.__actions = list()
        for group in self.__svc_groups:
            self.__cb_nr_action *= len(group.thresholds)
            self.__actions.append(group.thresholds)
            self.__thresholds.append(group.thresholds[0])
        self.__logger.info(f'一共 {self.__nr_group} 个组 {self.__cb_nr_action} 个动作')
        self.__learn = self.__scaler_cfg.learn
         
        # TODO 如果 learn 为 true 且发现启动了全局 locust
        # TODO cb_scaler 应立刻退出
        
        if self.__learn:
            self.__vw = vowpalwabbit.Workspace(f'--cb_explore {self.__cb_nr_action} '
                                               f'-l {self.__scaler_cfg.learning_rate} '
                                               f'--epsilon 0 '
                                               f'--nn {self.__scaler_cfg.nn_layers}', 
                                               quiet=True)
            
        self.__samples = list()
        self.__r_controller_thread = \
            threading.Thread(target=self.replicas_controller,
                             daemon=True)
        self.__last_modify_ts = None 
        self.__start_learning = threading.Semaphore()
        self.__start_learning.acquire()
        
        # 探索 loop 
        self.explore_count = {i + 1: 0 for i in range(self.__cb_nr_action)}
        
    def locust_requests_on_modified(self, event):
        ts = time.time()
        if self.__last_modify_ts is not None:
            if ts - self.__last_modify_ts < 10:
                return 
        self.__last_modify_ts = ts
        self.__logger.info('requests.csv 发生变化')
        self.__start_learning.release()
            
    def pre_start(self):
        
        # 启动副本数量控制器
        self.__logger.info('启动副本控制器')
        self.__r_controller_thread.start() 
        
        if self.__learn:
            self.__logger.info('启动 locust 与 otelcol')
            self.__locust.start()
            self.__logger.info('注册 oberserver')
            self.__locust.register_observer_on_requests(
                self.locust_requests_on_modified)
        
        return True 
        
    def start(self):
        self.__logger.info(f'AuthThreshold start ... learn = {self.__learn}') 
        self.__logger.info(f'cwd = {os.getcwd()}')
        
        if self.__learn:
            ok = self.explore_loop()
            if ok:
                self.learning_loop()
        else:
            self.evaluation_loop()
        
        self.__locust.stop()
        self.__logger.info('AutoThreshold exit.')
            
    def evaluation_loop(self):
        pass 
                
    def explore_loop(self):
        self.__logger.info('探索 loop 启动')
        samples_file = f'explore_samples-{self.__cfg.enabled_service_config}-{self.__workload}-{int(time.time())}.csv'
        
        samples_file = open(self.__priv_data_path / samples_file, 'w+')
        samples_file.write('rps,p99_rt,p95_rt,p50_rt,mean_rt,action,action_p,allocation,avg_cpu_usage,overallocation_ratio\n')
        samples_file.flush()
    
        for episode in range(self.__explore_episode):
            
            # 随机选择一个动作
            min_explore_count = min(self.explore_count.values())
            actions_with_min_explore_count = [k for k, v in self.explore_count.items() if v == min_explore_count]
            self.__logger.info(f'动作: {str(actions_with_min_explore_count)} 仅探索过 {min_explore_count} 次, 从这些动作中选择下一个探索动作')
            action = random.choice(actions_with_min_explore_count) 
            self.explore_count[action] += 1
            action_p = 1 / self.__cb_nr_action
            thresholds = self.get_thresholds(action)
            self.__logger.info(f'选择动作 {action} -> thresholds: {thresholds}')
            
            self.__logger.info(f'更新全局阈值配置为 {str(thresholds)}')
            self.__thresholds = thresholds
            
            # 等 Locust 统计文件发生变化
            self.__start_learning.acquire() 
            
            # 处理 locust 统计数据
            stats_df = self.__locust.read_requests()
            locust_sample = LocustSample(stats_df)
            
            # 获取平均 CPU 申请量
            allocation = 0
            for service in self.get_all_services_from_cfg():
                allocation_ = np.mean(self.fetch_cpu_requested_data(service, 60))
                allocation += allocation_ 
                
            # 获取平均 CPU 使用量
            cpu_usage = 0
            for service in self.get_all_services_from_cfg():
                cpu_usage_ = np.mean(self.fetch_cpu_usage_data(service, 60))
                cpu_usage += cpu_usage_
            
            # 将构建好的 sample 加入到 self.__samples
            self.__logger.info(
                f'rps:{locust_sample.request_per_second()}|'
                f'p99rt:{locust_sample.response_time("p99")}|'
                f'thresholds:{thresholds}|'
                f'cpu usage:{cpu_usage}|'
                f'allocation:{allocation}|'
                f'overalloc ratio:{(allocation - cpu_usage) / allocation}'
            )
            sample = (locust_sample, action, action_p, allocation, cpu_usage)
            self.__samples.append(sample)
            
            self.__logger.info(f'explore loop {episode}/{self.__explore_episode}') 
        
            # 将 self.__samples 导出为 explore_samples.csv 
            # overallocation_ratio = (allocation - avg_cpu_usage) / allocation 
            samples_file.write(
                f'{locust_sample.request_per_second()},'
                f'{locust_sample.response_time("p99")},'
                f'{locust_sample.response_time("p95")},'
                f'{locust_sample.response_time("p50")},'
                f'{locust_sample.response_time("mean")},'
                f'{action},'
                f'{action_p},'
                f'{allocation},'
                f'{cpu_usage},'
                f'{(allocation - cpu_usage) / allocation}\n'
            )
            samples_file.flush()
        
        self.__logger.info('探索 loop 退出')  
        return True 
    
    def learning_loop(self):
        
        return 
         
        self.__logger.info('学习 loop 启动')
        while True:
            # TODO 获取 RPS 与响应时间
            state_rps = 0
            state_rt = 0
            
            # CB 根据 RPS与响应时间 输出动作
            action, prob = self.get_action(state_rps, state_rt)
            thresholds = self.get_thresholds(action)
            self.__logger.info(f'action: {action} -> thresholds: {thresholds}')
            
            # TODO 更新阈值
            # self.__thresholds = thresholds
            
            # 无法立刻看到变化，需要缓一会
            time.sleep(2 * 60)
    
            # 获取响应时间
            result_rt = self.get_response_time()
            # 获取Pod资源使用量
            allocation = self.get_allocation()
            
            self.__logger.info(f'allocation = {allocation} | response time = {result_rt}')
            
            # TODO 计算 cost，cost越小越好
            cost = 0
            
            # 更新 CB 网络
            self.learn(action, cost, prob, state_rps, state_rt) 
                
    def fetch_cpu_usage_data(self, service_name, length):
        
        end = time.time()
        start = end - length
        dep_name = self.get_k8s_dep_name_from_cfg(service_name)
        namespace = self.get_k8s_namespace_from_cfg()
        
        retval = self.get_cpu_usage_from_prom(
            dep_name=dep_name, 
            namespace=namespace,
            start=start,
            end=end)
        
        return retval['value'].values 
    
    def fetch_cpu_requested_data(self, service_name, length):
        end = time.time()
        start = end - length
        dep_name = self.get_k8s_dep_name_from_cfg(service_name)
        namespace = self.get_k8s_namespace_from_cfg()
        
        retval = self.get_cpu_requested_from_prom(
            dep_name=dep_name, 
            namespace=namespace,
            state=GENESIS_IO_POD_STATE_RR,
            start=start,
            end=end)
        
        return retval['value'].values
    
    def get_cpu_request(self, service_name):
        cpu_request = 0
        resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
        for config in resources_config:
            if 'requests' in config['resources']:
                if 'cpu' in config['resources']['requests']:
                    cpu_request += int(config['resources']['requests']['cpu'][:-1])
        return cpu_request 
                
    def replicas_controller(self):
        
        def per_service_controller(service):
            # 获取历史 CPU 使用量
            history = self.fetch_cpu_usage_data(service, 3)
            #target = np.percentile(history, 90)
            target = np.max(history)
            
            # 获取 threshold
            index = 0
            for group in self.__svc_groups:
                if service in group.services:
                    break 
                index += 1
            threshold = self.__thresholds[index]
            
            # 获取 CPU request
            request = self.get_cpu_request(service) / 1000
            
            # 获取目标pod数量
            cap = request * threshold
            nr_pod = math.ceil(target / cap)
            self.__logger.debug(
                f'service {service} target CPU usage is {target}, '
                f'threshold is {threshold} and cap is {cap}. '
                f'Therefore, it need {nr_pod} RR pods.')
            
            # 设置 Pod 状态
            self.set_rr_pod_number(service, nr_pod)
        
        while True: 
            ret_futures = list()
            
            with futures.ThreadPoolExecutor() as executor:
                for service in self.get_all_services_from_cfg():
                    future = executor.submit(per_service_controller, service) 
                    ret_futures.append(future)
                    
                for future in as_completed(ret_futures):
                    future.result()
            
            time.sleep(1)
            
    def select_rcn_pods_to_rr(self, pods, number):
        ret_pods = list()
        if len(pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < number and len(pods) > 0:
            ret_pods.append(pods.pop(0))
            
        return ret_pods  
    
    def select_rr_pods_to_rcn(self, pods, number):
        ret_pods = list()
        if len(pods) == 0:
            return ret_pods 
        
        while len(ret_pods) < number and len(pods) > 0:
            ret_pods.append(pods.pop(0))
        
        return ret_pods  
    
    def set_pod_state_to_rr(self, pod_dict: Dict):
        __logger = self.__logger.getChild('Operation')
        
        try:
            pod = self.patch_k8s_pod(
                pod_dict["metadata"]["name"], pod_dict["metadata"]["namespace"],
                body=dict(
                    metadata=dict(
                        labels={
                            GENESIS_IO_STATE_LABEL: GENESIS_IO_POD_STATE_RR,
                            GENESIS_IO_ENDPOINT_LABEL: GENESIS_IO_ENDPOINT_UP
                        }
                    ),
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
                
        __logger.debug(
            f'set pod (name={pod.metadata.name} '
            f'namespace={pod.metadata.namespace}) '
            'to RR stats.'
        )
    
    def set_pod_state_to_rcn(self, pod: Dict):
        __l = self.__logger.getChild('Operation')
        
        try:
            pod = self.patch_k8s_pod(
                pod["metadata"]["name"], pod["metadata"]["namespace"],
                body=dict(
                    metadata=dict(
                        labels={
                            GENESIS_IO_STATE_LABEL: GENESIS_IO_POD_STATE_RCN,
                            GENESIS_IO_ENDPOINT_LABEL: GENESIS_IO_ENDPOINT_DOWN
                        }
                    )
                )
            )
        except ApiException as e:
            __l.debug(f'set pod to RCN stats failed. {e}')
            
        __l.debug(f'set pod (name={pod.metadata.name} '
                  f'namespace={pod.metadata.namespace}) '
                  'to RCN stats.')
                
    def set_rr_pod_number(self, service, number):
        max_number = self.get_service_max_replicas_from_cfg(service) 
        number = min(number, max_number)
        
        dep_name = self.get_k8s_dep_name_from_cfg(service)
        namespace = self.get_k8s_namespace_from_cfg()
        
        dep_obj = self.get_k8s_deployment(dep_name, namespace)
        
        match_labels = list()
        for k, v in dep_obj.spec.selector.match_labels.items():
            match_labels.append((k, v)) 
        resp = swift_list_pods_of_dep(namespace, match_labels[0][0], match_labels[0][1])
        if resp['status'] != 'success':
            raise Exception(resp.reason)
        
        pods = resp['pods'] 
        
        rr_pods, other_pods = \
            list(), list()
        for pod in pods:
            labels = pod['metadata']['labels']
            if GENESIS_IO_STATE_LABEL not in labels:
                raise Exception(
                    f'Pod(name={pod["metadata"]["name"]} '
                    f'namespace={pod["metadata"]["namespace"]}) '
                    f'do not has {GENESIS_IO_STATE_LABEL} label.'
                )
 
            elif labels[GENESIS_IO_STATE_LABEL] == GENESIS_IO_POD_STATE_RR:
                rr_pods.append(pod)
            else: 
                other_pods.append(pod)
                
        delta = number - len(rr_pods)
        if delta == 0:
            return 
        
        __start = time.time()
        
        ret_futures = list()
        with futures.ThreadPoolExecutor(max_workers=200) as executor:
            if delta > 0:
                pods = self.select_rcn_pods_to_rr(other_pods, delta)
            else:
                pods = self.select_rr_pods_to_rcn(rr_pods, -delta)
                
            for pod in pods:
                if delta > 0:
                    future = executor.submit(self.set_pod_state_to_rr, pod) 
                else:
                    future = executor.submit(self.set_pod_state_to_rcn, pod)
                ret_futures.append(future) 
            
            for future in as_completed(ret_futures):
                future.result()
                
        __end = time.time()
        self.__logger.debug(f'service {service} set pod state done. '
                            f'use {int(__end - __start)} seconds')
            
    def get_thresholds(self, action):
        actions = list()
        a = 1
        for i in reversed(range(self.__nr_group)):
            if i == self.__nr_group - 1:
                idx = int((action / a) % len(self.__actions[i]))
                threshold = self.__actions[i][idx - 1]
            else:
                idx = int((action / (a + 1)) % len(self.__actions[i]))
                threshold = self.__actions[i][idx]
            a *= len(self.__actions[i])
            actions.append(threshold)
        return list(reversed(actions)) 
        
    def get_action(self, rps, rt):
        distribution = self.__vw.predict(f'| rps:{rps} rt:{rt}') 
        action_index = np.random.choice(len(distribution), p=np.array(distribution) / sum(distribution))
        action = action_index + 1 
        prob = distribution[action_index]
        
        # action 从1开始
        return action, prob 
    
    def learn(self, action, cost, prob, rps, rt):
        sample = (
            f'{action}:{cost}:{prob} | rps:{rps} rt:{rt}'
        )
        self.__vw.learn(sample)
        