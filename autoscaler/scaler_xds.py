import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import random 
from typing import List, Dict, Any

# import grpc
import numpy as np

from .ts_predictor.estimator_xds import XDSEstimator
from .data import ServiceEndpointPair
# from proto import elastic_deployment_pb2 as ed_pb
# from proto import elastic_deployment_pb2_grpc as ed_grpc
from util.postgresql import PostgresqlDriver
from servicegraph import ServiceGraph

from kubernetes import client
configuration = client.Configuration()
configuration.verify_ssl = False 
configuration.cert_file = '/etc/kubernetes/ssl/cert/admin.pem'
configuration.key_file = '/etc/kubernetes/ssl/key/admin-key.pem'
configuration.host = 'https://192.168.137.138:6443'
client.Configuration.set_default(configuration)

import urllib3 
urllib3.disable_warnings()

WINDOW_SIZE_S = 5
DEFAULT_MAX_THREAD = 20
DEFAULT_MAX_REPLICAS = 10

L1_OFFSET = 5


class Scaler(object):
    def __init__(self, args, logger, service_configs: Dict):
        self.args = args
        
        self.logger = logger 

        self.service_configs = service_configs

        self.sql_driver = PostgresqlDriver(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_passwd,
            database=args.postgres_db
        )
        self.sg = ServiceGraph(neo4j_url=args.neo4j_url)
        self.neo4j_project_name = args.neo4j_project_name
        
    def get_cpu_limit_dec_step(self, service_name: str) -> int:
        if service_name not in self.service_configs:
            raise Exception(f'service: {service_name} not find in config file.')
        return self.service_configs.get(service_name).get('cpu-limit-dec-step')
        
    def get_cpu_limit_max(self, service_name: str) -> int:
        if service_name not in self.service_configs:
            raise Exception(f'service: {service_name} not find in config file.')
        return self.service_configs.get(service_name).get('cpu-limit-max')
    
    def get_response_time_slo(self, service_name: str, endpoint: str):
        return self.service_configs.get(service_name).get('response-time-slo').get(endpoint)
    
    def get_cpu_limit_min(self, service_name: str) -> int:
        if service_name not in self.service_configs:
            raise Exception(f'service: {service_name} not find in config file.')
        return self.service_configs.get(service_name).get('cpu-limit-min')

    def get_service_max_thread(self, service_name: str) -> int:
        if service_name not in self.service_configs:
            return DEFAULT_MAX_THREAD
        return self.service_configs.get(service_name).get('max-thread', DEFAULT_MAX_THREAD)

    def get_service_max_replicas(self, service_name: str) -> int:
        if service_name not in self.service_configs:
            return DEFAULT_MAX_REPLICAS
        return self.service_configs.get(service_name).get('max-replicas', DEFAULT_MAX_REPLICAS)

    def get_service_aimd_threshold(self, service_name: str) -> float:
        if service_name not in self.service_configs:
            return 2.0
        return float(self.service_configs.get(service_name).get('aimd-threshold', 2.0))

    def get_k8s_name(self, service_name: str) -> str:
        return self.service_configs.get(service_name).get('k8s-name')

    def get_k8s_deployment_replicas(self, api, k8s_name: str):
        while True:
            try:
                deployment = api.read_namespaced_deployment(
                    name=k8s_name, namespace=self.args.k8s_namespace)
                cur_replicas = deployment.spec.replicas
                break 
            except Exception as e:
                self.logger.error(e)
            time.sleep(random.random() * 5)
        return cur_replicas, deployment 

    def set_k8s_deployment_replicas(self, api, k8s_name: str, replicas: int, deployment = None):
        if deployment is None:
            while True:
                try:
                    deployment = api.read_namespaced_deployment(
                        name=k8s_name, namespace=self.args.k8s_namespace)
                    break 
                except Exception as e:
                    self.logger.error(e)
                time.sleep(random.random() * 5)
        while True:
            try:
                deployment.spec.replicas = replicas
                _ = api.patch_namespaced_deployment(
                    name=k8s_name, namespace=self.args.k8s_namespace,
                    body=deployment
                )
                break
            except:
                self.logger.error(f'update {k8s_name} deployment exception')
                deployment = api.read_namespaced_deployment(
                    name=k8s_name, namespace=self.args.k8s_namespace)
            time.sleep(random.random() * 5)

    def get_service_endpoints(self, service_name):
        svc_nodes = self.sg.match_services(self.neo4j_project_name, service_name)
        ep_nodes = self.sg.match_endpoints(svc_nodes[0])
        return [i['uri'] for i in ep_nodes]

    def get_target(self, service_name, endpoint_name):
        svc_nodes = self.sg.match_services(self.neo4j_project_name, service_name)
        ep_nodes = self.sg.match_endpoints(svc_nodes[0])
        this_ep_node = None
        for ep_node in ep_nodes:
            if ep_node['uri'] == endpoint_name:
                this_ep_node = ep_node
                break
        if this_ep_node is None:
            raise Exception(f'Endpoint {endpoint_name} do not exist in service {service_name}')
        res = list()
        downstream_ep_nodes = self.sg.match_target_endpoints(this_ep_node)
        for node in downstream_ep_nodes:
            svc_nodes = self.sg.match_service_by_endpoint(node)
            res.append(dict(
                service_name=svc_nodes[0]['name'],
                endpoint_name=node['uri']
            ))
        return res
    
    def get_all_services(self):
        return list(self.service_configs.keys())
    
    def get_concurrent_request_count(self, service_name, endpoint_name, window_size, limit):
        window_size_second = int(window_size / 1000)
        sql = f"SELECT " \
              f"    ((span_count / {window_size_second}) * rt_mean / 1000) AS v, " \
              f"    timestamp AS timestamp, " \
              f"    time AS time "  \
              f"FROM " \
              f"    span_stats " \
              f"WHERE " \
              f"    service_name = '{service_name}' " \
              f"    AND endpoint_name = '{endpoint_name}' " \
              f"    AND window_size = '{window_size}' "  \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT " \
              f"    {limit};"
        data = self.sql_driver.exec(sql)
        return data.v.values, data.timestamp.values, data.time.values 

    def get_service_time(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT pt_mean,rt_mean,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.rt_mean.values, data.pt_mean.values
    
    def get_endpoint_execution_time_95th(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT pt_95th,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.pt_95th.values
    
    def get_endpoint_response_time_mean(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT rt_mean,time  FROM response_time WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and (cluster = 'production' or cluster = 'cluster') " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.rt_mean.values
    
    def get_total_throughput(self, service_name, endpoint_name, window_size, limit):
        window_size_second = window_size / 1000
        sql = f"SELECT " \
              f"    (spen_count / {window_size_second}) AS throughput, " \
              f"    time AS time, " \
              f"    timestamp AS timestamp " \
              f"FROM " \
              f"    span_stats " \
              f"WHERE " \
              f"    service_name = '{service_name}' " \
              f"    AND endpoint_name = '{endpoint_name}' " \
              f"    AND window_size = '{window_size}' " \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT {limit};"
        
        data = self.sql_driver.exec(sql)
        
        return (
            data.throughput.values, 
            data.timestamp.values, 
            data.time.values 
        )
        
    def get_response_time(self, service_name, endpoint_name, window_size, limit):
        # TODO get response time from response_time table !!
        sql = f"SELECT " \
              f"    rt_mean AS response_time, "  \
              f"    timestamp AS timestamp, "  \
              f"    time AS time " \
              f"FROM " \
              f"    span_stats " \
              f"WHERE " \
              f"    service_name = '{service_name}' " \
              f"    AND endpoint_name = '{endpoint_name}' " \
              f"    AND window_size = '{window_size}' " \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT {limit};"
        
        data = self.sql_driver.exec(sql)
        
        return (
            data.response_time.values,
            data.timestamp.values,
            data.time.values
        ) 
    
    def get_response_time_ratio(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT " \
              f"    (rt_mean / pt_mean) AS ratio, "  \
              f"    timestamp AS timestamp, "  \
              f"    time AS time " \
              f"FROM " \
              f"    span_stats " \
              f"WHERE " \
              f"    service_name = '{service_name}' " \
              f"    AND endpoint_name = '{endpoint_name}' " \
              f"    AND window_size = '{window_size}' " \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT {limit};"
        
        data = self.sql_driver.exec(sql)
        
        return (
            data.ratio.values,
            data.timestamp.values,
            data.time.values
        ) 
    
    def get_traffic_intensity(self, service_name, endpoint_name,
                              dest_service_name, dest_endpoint_name,
                              window_size, limit):
        
        sql = f"SELECT " \
              f"    (callee_called_count / caller_called_count) AS intensity, " \
              f"    time AS time, " \
              f"    timestamp AS timestamp " \
              f"FROM "  \
              f"    rpc_stats "  \
              f"WHERE " \
              f"    service_name = '{service_name}' " \
              f"    AND endpoint_name = '{endpoint_name}' " \
              f"    AND dest_service_name = '{dest_service_name}' "  \
              f"    AND dest_endpoint_name = '{dest_endpoint_name}' " \
              f"    AND window_size = '{window_size}' " \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT {limit}"
              
        data = self.sql_driver.exec(sql)
        return data.intensity.values, data.timestamp.values, data.time.values 

    def get_ac_task_max_from_src(self, src_svc, src_ep, dst_svc, dst_ep, window_size, limit) -> np.ndarray:
        sql = f"SELECT ac_task_max,time FROM response_time2 WHERE " \
              f"src_sn = '{src_svc}' and src_en = '{src_ep}' and " \
              f"dst_sn = '{dst_svc}' and dst_en = '{dst_ep}' and " \
              f"window_size = {window_size} and " \
              f"cluster = 'production' " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit}"

        data = self.sql_driver.exec(sql)
        if len(data) == 0:
            return np.array([])

        return data.ac_task_max.values

    def get_ac_task_max(self, service_name, endpoint_name, window_size, limit) -> np.ndarray:
        sql = f"SELECT ac_task_max,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.ac_task_max.values

    def get_call_num(self, src_svc, src_ep, dst_svc, dst_ep):
        sql = f"SELECT request_num,time FROM response_time2 WHERE " \
              f"src_sn = '{src_svc}' and src_en = '{src_ep}' and " \
              f"dst_sn = '{dst_svc}' and dst_en = '{dst_ep}' and " \
              f"window_size = {WINDOW_SIZE_S * 1000} and " \
              f"cluster = 'production' " \
              f"ORDER BY _time DESC " \
              f"LIMIT 10"

        data = self.sql_driver.exec(sql)

        request_num_values: np.ndarray = data.request_num.values
        return np.mean(request_num_values), np.max(request_num_values), np.min(request_num_values)

    def get_call_num2(self, service_name, endpoint_name, limit):
        sql = f"SELECT request_num,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {WINDOW_SIZE_S * 1000} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.request_num.values


sync_traffic_intensity_threads = set()
sync_traffic_intensity_threads_lock = threading.Lock()
traffic_intensity_cache = dict()
traffic_intensity_cache_lock = threading.Lock()

USE_ENBPI = True 

class SwiftKubeAutoScaler(Scaler):
    def __init__(self, args, logger, 
                 root_endpoints: List[ServiceEndpointPair], 
                 service_configs: Dict,
                 estimator: XDSEstimator):

        super().__init__(args, logger, service_configs)
        assert estimator is not None 
        self.estimator = estimator

        self.root_endpoints = root_endpoints
        self.service_configs = service_configs

        self.realtime_controller_sync_period_sec = 5
        self.predictive_controller_sync_preiod_sec = 60 

        self.args = args
        self.logger = logger
        
        self.k8s_apps_v1 = client.AppsV1Api() 

        self.prev_sttf_upper_bound = dict()

        self.sql_driver = PostgresqlDriver(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_passwd,
            database=args.postgres_db
        )
        self.sg = ServiceGraph(neo4j_url=args.neo4j_url)
        self.neo4j_project_name = args.neo4j_project_name
        
        self.pred_time_stat_file = open('stat_pred_time.csv', 'w+')
        self.estm_time_stat_file = open('stat_estm_time.csv', 'w+')
        self.total_time_stat_file = open('stat_total_time.csv', 'w+')
        
        self.pred_time_stat_file.write('value\n')
        self.pred_time_stat_file.flush()
        self.estm_time_stat_file.write('value\n')
        self.estm_time_stat_file.flush()
        self.total_time_stat_file.write('value\n')
        self.total_time_stat_file.flush()
        
        self.stat_lock = threading.Lock()

    def start(self):
        self.logger.info('SwiftKubeAutoScaler starting ...')
        
        predictive_controller_last_time = None 
        
        g_predicted_throughput = dict()
        g_predicted_throughput_lock = threading.Lock()
        
        while True:
            start_time = time.time()            
            
            # 0. Check the status of frontend endpoint
            check_threads = list()
            status = dict()
            
            # 0.0 Start checking threads 
            for pair in self.root_endpoints:
                service_name = pair.service_name
                endpoint_name = pair.endpoint_name
                
                t = threading.Thread(target=self.check_frontend_endpoint_status,
                                     args=(service_name, endpoint_name, status),
                                     daemon=True)
                check_threads.append(t)
                t.start()
                
            for thread in check_threads:
                thread.join()
            
            """ extracted_status 
            {
                "service_name": {
                    "endpoint_name": status 
                }
            }
            """
            extracted_status: Dict[str, Dict[str, Any]] = dict()
            
            # 0.1 Extract status 
            for key in status:
                splits = str(key).split(':')
                service_name = splits[0]
                endpoint_name = splits[1]
                
                _status = status[key]
                if service_name not in extracted_status:
                    extracted_status[service_name] = dict()
                assert endpoint_name not in extracted_status[service_name]
                extracted_status[service_name][endpoint_name] = _status 
                
            # TODO If SLO violation occurs, scale-up is required
            
            # 1. Check the status of each service
            checking_result = self.check_services_status()
            
            # TODO If there is resource waste, it is necessary to scale-down
            
            # 2. Predict frontend endpoints throughput if timeout 
            current_time = time.time()
            if predictive_controller_last_time is None or \
                current_time - predictive_controller_last_time > self.predictive_controller_sync_preiod_sec:
                    
                self.predictive_controller_last_time = current_time
                predicted_throughput = dict()
                predict_throughput_threads = list()
                
                for pair in self.root_endpoints:
                    t = threading.Thread(target=self.predict_throughput,
                                         args=(pair.service_name, pair.endpoint_name, predicted_throughput))
                    predict_throughput_threads.append(t)
                    t.start()
                
                for thread in predict_throughput_threads:
                    thread.join()
                    
                g_predicted_throughput_lock.acquire()
                for key in predicted_throughput:
                    g_predicted_throughput[key] = predicted_throughput[key]
                g_predicted_throughput_lock.release()
            
            # 3. Estimate throughput of each service 
            estimated_throughput: Dict[str, Dict[str, int]] = dict()
            estimated_throughput_lock = threading.Lock()
            estimate_throughput_threads = list()
            
            for pair in self.root_endpoints:
                service_name = pair.service_name
                endpoint_name = pair.endpoint_name
                
                t = threading.Thread(target=self.estimate_throughput,
                                     args=(service_name, endpoint_name, 
                                           g_predicted_throughput[f'{service_name}:{endpoint_name}'], 
                                           estimated_throughput, estimated_throughput_lock),
                                     daemon=True)
                estimate_throughput_threads.append(t)
                t.start()

            for thread in estimate_throughput_threads:
                thread.join()

            # 4. Aggregate estimated results 
            aggregated_estimated_throughput = dict()
            for key in estimated_throughput:
                res = estimated_throughput.get(key)
                for service_name in res:
                    aggregated_estimated_throughput[service_name] = \
                        aggregated_estimated_throughput.get(service_name, 0) + res[service_name]
                    
            self.logger.info('parallelism needed result:')
            for service_name in aggregated_estimated_throughput:
                self.logger.info(f'\t{service_name} need {format(g_res[service_name], ".2f")} parallelism')

            self.do_l1_scaling(g_res)

            end_time = time.time()

            if end_time - start_time < self.realtime_controller_sync_period_sec:
                time.sleep(self.realtime_controller_sync_period_sec - (end_time - start_time))

    def do_l1_scaling(self, results: Dict[str, int]):
        for service_name in results:
            max_thread = self.get_service_max_thread(service_name)
            k8s_name = self.get_k8s_name(service_name)
            
            cur_replicas, deployment = self.get_k8s_deployment_replicas(
                self.k8s_apps_v1, k8s_name
            )

            thread_need = results.get(service_name)
            running_replicas_need = math.ceil(thread_need / max_thread)

            # TODO 
            #replicas_need = cur_replicas
            #if cur_replicas < running_replicas_need + 3:
            #    replicas_need = running_replicas_need + 3
            #replicas_need = min(self.get_service_max_replicas(service_name), replicas_need)
            
            replicas_need = min(self.get_service_max_replicas(service_name), running_replicas_need)

            # TODO
            # elif all_replicas > running_replicas_need + 6:
            #     replicas_need = running_replicas_need + 6

            #self.logger.debug(f'(MicroKube L1 Autoscaler) ==> {service_name} need {running_replicas_need} running replicas.')
            self.logger.debug(f'(Microservice Autoscaler) ==> {service_name} need {replicas_need} replicas.')

            #self.set_running_replicas(service_name, running_replicas_need)
            #self.set_replicas(service_name, replicas_need)
            self.set_k8s_deployment_replicas(
                self.k8s_apps_v1, k8s_name, replicas_need, deployment
            )
            
    def predict_throughput(self, service_name, endpoint_name, results):
        
        predict_value, upper_bound, _, last_value, _ = \
            self.estimator.predict(service_name, endpoint_name)

        key = f'{service_name}:{endpoint_name}'
        # thread_need = max(last_value, self.prev_sttf_upper_bound.get(key, 0))
        if USE_ENBPI:
            throughput = max(last_value, upper_bound)
        else:
            throughput = predict_value + 10
        # self.prev_sttf_upper_bound[key] = upper_bound
        results[key] = throughput

    def estimate_throughput(self, service_name: str, endpoint_name: str, 
                            target_throughput, 
                            results, results_lock):
        throughputs = dict()

        target_throughput += L1_OFFSET
        
        estm_t0 = time.time()
        self.do_estimate_throughput(service_name, endpoint_name, target_throughput, throughputs)
        estm_t1 = time.time()
        self.logger.debug(f'SwiftKubeAutoScaler use {estm_t1 - estm_t0} seconds estimate throughput')

        results_lock.acquire()
        key = f'estimate_throughput-{service_name}:{endpoint_name}'
        results[key] = throughputs 
        results_lock.release()
        
    def sync_traffic_intensity(self, src_sn, src_en, dst_sn, dst_en, cache_key):
        def do_sync():
            key = f'sync_traffic_intensity-{src_sn}/{src_en}=>{dst_sn}/{dst_en}'
            sync_traffic_intensity_threads_lock.acquire()
            if key in sync_traffic_intensity_threads:
                sync_traffic_intensity_threads_lock.release()
                return 
            sync_traffic_intensity_threads.add(key)
            sync_traffic_intensity_threads_lock.release()
            
            traffic_intensity_cache_lock.acquire()
            traffic_intensity, _, _ = self.get_traffic_intensity(
                src_sn, src_en, dst_sn, dst_en, window_size=60000, limit=1
            )
            
            if len(traffic_intensity) == 0:
                print(f"Traffic intensity data from "  \
                      f"{src_sn}/{src_en} to {dst_sn}/{dst_en} not enough.")
                traffic_intensity_cache_lock.release()
                return 
            
            traffic_intensity_cache[cache_key] = dict(
                value=traffic_intensity,
                last_sync_time=time.time()
            )
            
            sync_traffic_intensity_threads_lock.acquire()
            sync_traffic_intensity_threads.remove(key)
            sync_traffic_intensity_threads_lock.release()
            
            traffic_intensity_cache_lock.release()
        t = threading.Thread(target=do_sync, daemon=True)
        t.start()
        
    def check_frontend_endpoint_status(self, service_name, endpoint_name, status):
        """
        Status: 
        {
            "slo-violation': True or False (default False)
        }
        """
        key = f'{service_name}:{endpoint_name}'
        
        _status = dict()
        
        # 1. SLO violation check  
        response_time, _, _ = self.get_response_time(service_name, endpoint_name, 5000, 10)
        slo = self.get_response_time_slo(service_name, endpoint_name)
        if response_time[0] > slo:
            _status['slo-violation'] = True 
        status[key] = _status 
        
    def check_services_status(self):
        services = self.get_all_services()
        pool = ThreadPoolExecutor(max_workers=len(services))
        futures = dict()
        results = dict()
        for service_name in services:
            future = pool.submit(self.check_service_status, service_name)
            futures[service_name] = future
            
        for service_name in futures:
            results[service_name] = futures[service_name].result()
        
        return results 
    
    def check_service_status(self, service_name):
        """
        Status:
        {
            "overallocation": True of False,
            "parallelism": int,
            "concurrency": float 
        }
        """
        max_thread = self.get_service_max_thread(service_name)
        replicas = self.get_k8s_deployment_replicas(self.k8s_apps_v1,
                                                    self.get_k8s_name(service_name))
        
        endpoints = self.get_service_endpoints(service_name)
        total_concurrency = 0
        for endpoint in endpoints:
            value, _, _ = self.get_concurrent_request_count(service_name, endpoint, 5000, 10)
            concurrency = np.mean(value)
            total_concurrency += concurrency 
            
        parallelism = replicas * max_thread
        
        return dict(
            overallocation=(total_concurrency < parallelism),
            parallelism=parallelism,
            concurrency=total_concurrency
        )
        

    def do_estimate_throughput(self, 
                              service_name: str, 
                              endpoint_name: str, 
                              throughput, 
                              throughputs: Dict):
        throughputs[service_name] = throughputs.get(service_name, 0) + throughput

        targets = self.get_target(service_name, endpoint_name)
        for tgt_service in targets:
            tgt_service_name = tgt_service.get('service_name')
            tgt_endpoint_name = tgt_service.get('endpoint_name')

            cache_key = f'traffic_intensity-{service_name}:{endpoint_name}=>{tgt_service_name}:{tgt_endpoint_name}'
            
            traffic_intensity_cache_lock.acquire()
            if (cache_key not in traffic_intensity_cache) or \
                (time.time() - traffic_intensity_cache.get(cache_key).get('last_sync_time') > 5 * 60):
                self.sync_traffic_intensity(
                    service_name, endpoint_name,
                    tgt_service_name, tgt_endpoint_name,
                    cache_key
                )
            if cache_key in traffic_intensity_cache:
                weight = traffic_intensity_cache.get(cache_key).get('value')
            else:
                traffic_intensity_cache_lock.release()
                continue
            traffic_intensity_cache_lock.release()
            tgt_throughput = throughput * weight

            self.do_estimate_throughput(tgt_service_name, tgt_endpoint_name, tgt_throughput, throughputs)


COUNT = 1
SLEEP = 10

exclude_services = {
    'locust', 'CastInfoService', 'MovieInfoService', 'PlotService'
}

class AIMDHScaler(Scaler):
    def __init__(self, args, logger, service_configs: Dict):

        super().__init__(args, logger, service_configs)
        self.args = args
        self.sg = ServiceGraph(neo4j_url=args.neo4j_url)

        self.k8s_apps_v1 = client.AppsV1Api() 

        self.a = 1
        self.b = 1.5
        
        self.cache = dict()
        
        self.logger = logger 

    def start(self):
        self.logger.info('Start AIMD-H Autoscaler.')
        services = self.sg.match_services(self.args.neo4j_project_name)
        threads = list()

        for svc in services:
            if svc.get('name') in exclude_services:
                continue
            t = threading.Thread(target=self.do_scaling, args=(svc, ), daemon=True)
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()

    def do_scaling(self, service):
        endpoints = self.sg.match_endpoints(service)
        max_replicas = self.get_service_max_replicas(service.get('name'))
        
        service_name = service.get('name')
        
        self.cache[service_name] = dict()
        self.cache[service_name]['__count__'] = COUNT 

        while True:
            queueing = False
            ok = False
            
            for ep in endpoints:
                ep_uri = ep.get('uri')
                rt_mean_values = self.get_endpoint_response_time_mean(service_name, ep_uri, 5000, 60)
                if len(rt_mean_values) == 0:
                    continue 
                rt_mean = np.mean(rt_mean_values)
                if ep_uri not in self.cache[service_name]:
                    """ initialize """
                    self.logger.info(f'[{service.get("name")}:{ep.get("uri")}-INIT] target response time is {rt_mean}.')
                    self.cache[service_name][ep_uri] = rt_mean 
                    continue 
                else:
                    ok = True 
                    tgt_rt_mean = self.cache[service_name][ep_uri]
                    if rt_mean > (tgt_rt_mean * 1.1):
                        queueing = True  
            
            if not ok:
                time.sleep(SLEEP)
                continue
            
            k8s_name = self.get_k8s_name(service.get('name'))
            while True:
                try:
                    deployment = self.k8s_apps_v1.read_namespaced_deployment(
                        name=k8s_name, namespace=self.args.k8s_namespace)
                    cur_replicas = deployment.spec.replicas
                    break 
                except Exception as e:
                    self.logger.error(e)
                time.sleep(random.random() * 5)

            if not queueing:
                if self.cache[service_name]['__count__'] == 0:
                    target_replicas = max(1, cur_replicas - self.a)
                    self.cache[service_name]['__count__'] = COUNT 
                else:
                    target_replicas = cur_replicas 
            else:
                target_replicas = min(max_replicas, cur_replicas * self.b)
                self.cache[service_name]['__count__'] = COUNT 
            
            target_replicas = math.ceil(target_replicas)
            if target_replicas == cur_replicas:
                self.logger.info(f'[{service.get("name")}] nothing done.')
            elif target_replicas < cur_replicas:
                self.logger.info(f'[{service.get("name")}] decrease {self.a} replicas.')
            else:
                self.logger.info(f'[{service.get("name")}] increase {self.b * cur_replicas - cur_replicas} replicas.')

            if target_replicas != cur_replicas:
                while True:
                    try:
                        deployment.spec.replicas = target_replicas
                        _ = self.k8s_apps_v1.patch_namespaced_deployment(
                            name=k8s_name, namespace=self.args.k8s_namespace,
                            body=deployment
                        )
                        break
                    except Exception as e:
                        self.logger.error(f'update {service.get("name")} exception: {str(e)}')
                        deployment = self.k8s_apps_v1.read_namespaced_deployment(
                            name=k8s_name, namespace=self.args.k8s_namespace)
                    time.sleep(random.random() * 5)
            
            self.cache[service_name]['__count__'] -= 1
            time.sleep(SLEEP)

class AIMDVScaler(Scaler):
    def __init__(self, args, logger, service_configs: Dict):
        super().__init__(args, logger, service_configs)
        self.args = args
        self.sg = ServiceGraph(neo4j_url=args.neo4j_url)
        
        self.k8s_apps_v1 = client.AppsV1Api()
        
        self.cache = dict()

        self.b = 1.2
        
        self.logger = logger 

    def start(self):
        print('Start AIMD-V Autoscaler.')
        services = self.sg.match_services(self.args.neo4j_project_name)
        threads = list()

        for svc in services:
            if svc.get('name') in exclude_services:
                continue
            t = threading.Thread(target=self.do_scaling, args=(svc, ), daemon=True)
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()

    def do_scaling(self, service):
        endpoints = self.sg.match_endpoints(service)
        max_replicas = self.get_service_max_replicas(service.get('name'))
        
        service_name = service.get('name')
        
        self.cache[service_name] = dict()
        self.cache[service_name]['__count__'] = COUNT 

        while True:
            queueing = False
            ok = False
            for ep in endpoints:
                ep_uri = ep.get('uri')
                rt_mean_values = self.get_endpoint_response_time_mean(service_name, ep_uri, 5000, 3)
                if len(rt_mean_values) == 0:
                    continue
                rt_mean = np.mean(rt_mean_values)
                if ep_uri not in self.cache[service_name]:
                    """ initialize """
                    self.logger.info(f'[{service.get("name")}:{ep.get("uri")}-INIT] target response time is {rt_mean}.')
                    self.cache[service_name][ep_uri] = rt_mean 
                    continue 
                else:
                    ok = True 
                    tgt_rt_mean = self.cache[service_name][ep_uri]
                    if rt_mean > (tgt_rt_mean * 1.5):
                        queueing = True 
                        
            if not ok:
                time.sleep(SLEEP)
                continue
            
            k8s_name = self.get_k8s_name(service.get('name'))
            not_ready = False 
            while True:
                try:
                    deployment = self.k8s_apps_v1.read_namespaced_deployment(
                        name=k8s_name, namespace=self.args.k8s_namespace)
                    cur_replicas = deployment.spec.replicas
                    if deployment.status.ready_replicas != cur_replicas:
                        not_ready = True 
                    if deployment.status.updated_replicas != cur_replicas:
                        not_ready = True 
                    break 
                except Exception as e:
                    self.logger.error(e)
                time.sleep(random.random() * 5)
            if not_ready:
                time.sleep(SLEEP)
                continue 
            container = None
            for c in deployment.spec.template.spec.containers:
                if c.name == k8s_name:
                    container = c
                    break
            if container is None:
                raise Exception(f'can not find container {service.get("name")}')

            cur_cpu_limit = container.resources.limits.get('cpu')
            if not cur_cpu_limit.endswith('m'):
                cur_cpu_limit = int(cur_cpu_limit) * 1000
            else:
                cur_cpu_limit = int(cur_cpu_limit[:-1])
            cur_all_cpu = int(cur_cpu_limit * cur_replicas)
            cur_all_cpu = min(self.get_cpu_limit_max(service_name) * cur_replicas, cur_all_cpu)

            if not queueing:
                if self.cache[service_name]['__count__'] == 0:
                    target_cpu_limit = max(self.get_cpu_limit_min(service_name), cur_all_cpu - cur_replicas * self.get_cpu_limit_dec_step(service_name))
                    self.cache[service_name]['__count__'] = COUNT 
                else:
                    self.cache[service_name]['__count__'] -= 1
                    time.sleep(SLEEP)
                    continue 
            else:
                target_cpu_limit = min(max_replicas * self.get_cpu_limit_max(service_name), math.ceil(cur_all_cpu * self.b))
                self.cache[service_name]['__count__'] = COUNT 

            while True:
                per_container_cpu = int(target_cpu_limit / cur_replicas)
                if per_container_cpu < self.get_cpu_limit_min(service_name):
                    target_replicas = math.ceil(target_cpu_limit / self.get_cpu_limit_min(service_name))
                    if target_replicas == cur_replicas:
                        target_replicas = cur_replicas - 1 
                    target_cpu_limit = self.get_cpu_limit_min(service_name)
                elif per_container_cpu > self.get_cpu_limit_max(service_name):
                    target_replicas = math.ceil(target_cpu_limit / self.get_cpu_limit_max(service_name))
                    target_cpu_limit = self.get_cpu_limit_max(service_name)
                else:
                    target_replicas = cur_replicas
                    target_cpu_limit = per_container_cpu

                target_replicas = min(max_replicas, target_replicas)
                target_replicas = max(1, target_replicas)
                for container in deployment.spec.template.spec.containers:
                    if container.name == k8s_name:
                        self.logger.info(f'service {k8s_name} cpu {cur_cpu_limit}m -> {target_cpu_limit}m\treplicas {cur_replicas} -> {target_replicas}')
                        container.resources.limits = { 'cpu': f'{target_cpu_limit}m' }
                        break 
                deployment.spec.replicas = target_replicas
                try:
                    _ = self.k8s_apps_v1.patch_namespaced_deployment(
                        name=k8s_name, namespace=self.args.k8s_namespace,
                        body=deployment
                    )
                    break
                except Exception as e:
                    deployment = self.k8s_apps_v1.read_namespaced_deployment(name=k8s_name, namespace=self.args.k8s_namespace)
                    self.logger.error(e)
                time.sleep(random.random() * 5)
            self.cache[service_name]['__count__'] -= 1
            time.sleep(SLEEP)
