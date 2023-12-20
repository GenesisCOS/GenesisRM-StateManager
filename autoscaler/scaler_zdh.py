import math
import time
import threading
import random 
from typing import List, Dict, Any

# import grpc
import numpy as np

from .ts_predictor.estimator import Estimator as SttfEstimator
from .data import ServiceEndpointPair
from util.postgresql import PostgresqlDriver
from servicegraph import ServiceGraph

from kubernetes import client, dynamic
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

    def set_swiftdeployment_replicas():
        pass 
    
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
            
    def get_running_replicas(self, service_name: str):
        raise Exception( "Not implemented" )

    def set_running_replicas(self, service_name: str, replicas: int):
        raise Exception( "Not implemented." ) 

    def get_downstream(self, service_name, endpoint_name):
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
        downstream_ep_nodes = self.sg.match_downstream_endpoints(this_ep_node)
        for node in downstream_ep_nodes:
            svc_nodes = self.sg.match_service_by_endpoint(node)
            res.append(dict(
                service_name=svc_nodes[0]['name'],
                endpoint_name=node['uri']
            ))
        return res

    def get_service_time(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT " \
              f"    pt_mean,rt_mean,time " \
              f"FROM " \
              f"    service_time_stats " \
              f"WHERE " \
              f"    sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"    and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.rt_mean.values, data.pt_mean.values
    
    def get_endpoint_execution_time_95th(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT " \
              f"    pt_95th,time " \
              f"FROM " \
              f"    service_time_stats " \
              f"WHERE " \
              f"    sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"    and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.pt_95th.values
    
    def get_endpoint_response_time_mean(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT " \
              f"    rt_mean,time  "  \
              f"FROM " \
              f"    response_time " \
              f"WHERE " \
              f"    sn = '{service_name}' and en = '{endpoint_name}' " \
              f"    and (cluster = 'production' or cluster = 'cluster') " \
              f"    and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.rt_mean.values

    def get_ac_task_max_from_src(self, src_svc, src_ep, dst_svc, dst_ep, window_size, limit) -> np.ndarray:
        sql = f"SELECT " \
              f"    ac_task_max,time "  \
              f"FROM " \
              f"    response_time2 " \
              f"WHERE " \
              f"    src_sn = '{src_svc}' and src_en = '{src_ep}' and " \
              f"    dst_sn = '{dst_svc}' and dst_en = '{dst_ep}' and " \
              f"    window_size = {window_size} and " \
              f"    cluster = 'production' " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit}"

        data = self.sql_driver.exec(sql)
        if len(data) == 0:
            return np.array([])

        return data.ac_task_max.values

    def get_ac_task_max(self, service_name, endpoint_name, window_size, limit) -> np.ndarray:
        sql = f"SELECT " \
              f"    ac_task_max,time " \
              f"FROM " \
              f"    service_time_stats " \
              f"WHERE " \
              f"    sn = '{service_name}' and en = '{endpoint_name}' " \
              f"    and cluster = 'production' " \
              f"    and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.ac_task_max.values

    def get_call_num(self, src_svc, src_ep, dst_svc, dst_ep):
        sql = f"SELECT " \
              f"    request_num,time " \
              f"FROM " \
              f"    response_time2 " \
              f"WHERE " \
              f"    src_sn = '{src_svc}' and src_en = '{src_ep}' and " \
              f"    dst_sn = '{dst_svc}' and dst_en = '{dst_ep}' and " \
              f"    window_size = {WINDOW_SIZE_S * 1000} and " \
              f"    cluster = 'production' " \
              f"ORDER BY _time DESC " \
              f"LIMIT 10"

        data = self.sql_driver.exec(sql)

        request_num_values: np.ndarray = data.request_num.values
        return np.mean(request_num_values), np.max(request_num_values), np.min(request_num_values)

    def get_call_num2(self, service_name, endpoint_name, limit):
        sql = f"SELECT " \
              f"    request_num,time " \
              f"FROM " \
              f"    service_time_stats " \
              f"WHERE " \
              f"    sn = '{service_name}' and en = '{endpoint_name}' " \
              f"    and cluster = 'production' " \
              f"    and window_size = {WINDOW_SIZE_S * 1000} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.request_num.values


cache = dict()
cache_lock = threading.Lock()

USE_ENBPI = True 

class MicroKubeScaler(Scaler):
    def __init__(self, args, logger, 
                 root_endpoints: List[ServiceEndpointPair], 
                 service_configs: Dict,
                 sttf_estimator: SttfEstimator | None, 
                 lttf_estimator: Any | None):

        super().__init__(args, logger, service_configs)
        if sttf_estimator is not None:
            self.logger.info('use short term time-series estimator')
        self.sttf_estimator = sttf_estimator

        if lttf_estimator is not None:
            self.logger.info('use long term time-series estimator')
        self.lttf_estimator = lttf_estimator

        self.root_endpoints = root_endpoints
        self.service_configs = service_configs

        self.l1_sync_period_sec = args.l1_sync_period_sec
        self.l2_sync_period_sec = args.l2_sync_period_sec

        self.args = args
        self.logger = logger
        
        self.k8s_apps_v1 = client.AppsV1Api() 
        
        dynamic_kube_client = dynamic.DynamicClient()
        self.swiftdeployment_api = dynamic_kube_client.resources.get(api_version='swiftkube.io/v1alpha1', kind='SwiftDeployment')

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
        self.logger.info('MicroKube scaler starting ...')
        l2_start_time = time.time()
        cur_time = time.time()
        while True:
            start_time = time.time()

            g_res: Dict[str, int] = dict()
            results: Dict[str, Dict[str, int]] = dict()
            results_lock = threading.Lock()

            threads = list()
            total_t0 = time.time()
            for pair in self.root_endpoints:
                if self.sttf_estimator is not None:
                    t = threading.Thread(target=self.start_l1_scaling,
                                         args=(pair.service_name, pair.endpoint_name, results, results_lock),
                                         daemon=True)
                    threads.append(t)
                    t.start()

                if self.lttf_estimator is not None and (cur_time - l2_start_time) > self.l2_sync_period_sec:
                    l2_start_time = time.time()
                    pass

            for thread in threads:
                thread.join()

            for key in results:
                res = results.get(key)
                for service_name in res:
                    g_res[service_name] = g_res.get(service_name, 0) + res[service_name]
            total_t1 = time.time()
            self.stat_lock.acquire()
            self.total_time_stat_file.write(str((total_t1 - total_t0) * 1000) + '\n')
            self.total_time_stat_file.flush()
            self.stat_lock.release()
            self.logger.info('parallelism needed result:')
            for service_name in g_res:
                self.logger.info(f'\t{service_name} need {format(g_res[service_name], ".2f")} parallelism')

            self.do_l1_scaling(g_res)

            end_time = time.time()
            cur_time = end_time
            if end_time - start_time < self.l1_sync_period_sec:
                time.sleep(self.l1_sync_period_sec - (end_time - start_time))

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

    def start_l1_scaling(self, service_name: str, endpoint_name: str, results, results_lock):
        predict_value, upper_bound, _, last_value, use_time = self.sttf_estimator.predict(service_name, endpoint_name)

        key = f'{service_name}:{endpoint_name}'
        # thread_need = max(last_value, self.prev_sttf_upper_bound.get(key, 0))
        if USE_ENBPI:
            thread_need = max(last_value, upper_bound)
        else:
            thread_need = predict_value + 10
        # self.prev_sttf_upper_bound[key] = upper_bound

        res = dict()

        t0 = time.time()
        thread_need += L1_OFFSET
        estm_t0 = time.time()
        self.cal_parallelism(service_name, endpoint_name, thread_need, res)
        estm_t1 = time.time()
        self.logger.debug(f'scaling use {time.time() - t0} seconds calculate concurrency')
        
        self.stat_lock.acquire()
        self.estm_time_stat_file.write(str((estm_t1 - estm_t0) * 1000) + '\n')
        self.estm_time_stat_file.flush()
        self.pred_time_stat_file.write(str(use_time * 1000) + '\n')
        self.pred_time_stat_file.flush()
        self.stat_lock.release()

        results_lock.acquire()
        key = f'l1-{service_name}:{endpoint_name}'
        results[key] = res
        results_lock.release()
        
    def refresh_weight(self, src_sn, src_en, dst_src, dst_en, cache_key):
        def do_refresh():
            cache_lock.acquire()
            ac_task_max_from_src = self.get_ac_task_max_from_src(
                src_sn, src_en, dst_src, dst_en, window_size=60000, limit=2)
            if len(ac_task_max_from_src) == 0:
                raise Exception('Data not enough.')
            ac_task_max = self.get_ac_task_max(src_sn, src_en, window_size=60000, limit=2)
            weight = min(1., float(np.mean(ac_task_max_from_src / ac_task_max)))
            cache[cache_key] = dict(
                value=weight,
                last_sync_time=time.time()
            )
            cache_lock.release()
        t = threading.Thread(target=do_refresh, daemon=True)
        t.start()

    def cal_parallelism(self, 
                        service_name: str, 
                        endpoint_name: str, 
                        thread_need, 
                        res: Dict):
        res[service_name] = res.get(service_name, 0) + thread_need

        downstream = self.get_downstream(service_name, endpoint_name)
        for downstream_service in downstream:
            d_service_name = downstream_service.get('service_name')
            d_endpoint_name = downstream_service.get('endpoint_name')

            cache_key = f'l1-{service_name}:{endpoint_name}=>{d_service_name}:{d_endpoint_name}'
            
            cache_lock.acquire()
            if (cache_key not in cache) or (time.time() - cache.get(cache_key).get('last_sync_time') > 10 * 60):
                self.refresh_weight(
                    service_name,
                    endpoint_name,
                    d_service_name,
                    d_endpoint_name,
                    cache_key
                )
            if cache_key in cache:
                weight = cache.get(cache_key).get('value')
            else:
                cache_lock.release()
                continue
            cache_lock.release()
            _thread_need = thread_need * weight

            self.logger.debug(cache_key, f'weight: {cache.get(cache_key).get("value")}')
            self.cal_parallelism(d_service_name, d_endpoint_name, _thread_need, res)


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
