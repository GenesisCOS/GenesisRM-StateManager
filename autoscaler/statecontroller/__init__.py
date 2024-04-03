import time 
import random 
import json 
import urllib3 
import requests 
from logging import Logger 
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd 
from omegaconf import DictConfig, OmegaConf
from kubernetes import client, dynamic
from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.api.apps_v1_api import AppsV1Api

from ..util.postgresql import PostgresqlDriver
from ..servicegraph import ServiceGraph

urllib3.disable_warnings()


def labelselector_dict_to_str(selector: Dict) -> str:
    retval = ''
    first = True 
    for key in selector:
        if first:
            first = False 
        else:
            retval += ','
        retval += f'{key}={selector[key]}'
    return retval 


class ServiceEndpointPair(object):
    def __init__(self, service_name, endpoint_name) -> None:
        self.__service_name = service_name
        self.__endpoint_name = endpoint_name
    
    @property
    def service_name(self):
        return self.__service_name
    
    @property
    def endpoint_name(self):
        return self.__endpoint_name
    
    
class ServiceConfig(object):
    def __init__(self, cfg: DictConfig, logger: Logger) -> None:
        self.__logger = logger.getChild('ServiceConfig')
        self.__svc_cfg = cfg[cfg.enabled_service_config]
        
        self.__service_configs = dict()
        self.__frontend_endpoints = dict()
        
        for _svc_cfg in self.__svc_cfg.configs:
            svc_name = _svc_cfg.service_name 
            self.__service_configs[svc_name] = _svc_cfg 
            
        for _frontend in self.__svc_cfg.frontend_endpoints:
            self.__frontend_endpoints[_frontend.service_name] = _frontend.endpoint_name 
            
    def get_service_max_worker_from_cfg(self, service_name: str) -> int:
        assert service_name in self.__service_configs
        return self.__service_configs.get(service_name).worker.max 

    def get_service_max_replicas_from_cfg(self, service_name: str) -> int:
        assert service_name in self.__service_configs
        return self.__service_configs.get(service_name).replicas.max 
    
    def get_service_worker_target_utilization_from_cfg(self, service_name: str) -> int:
        assert service_name in self.__service_configs
        return self.__service_configs.get(service_name).worker.target_utilization 
    
    def get_k8s_namespace_from_cfg(self) -> str:
        return self.__svc_cfg.k8s.namespace

    def get_k8s_dep_name_from_cfg(self, service_name: str) -> str:
        assert service_name in self.__service_configs
        return self.__service_configs.get(service_name).get('k8s_dep_name')
    
    def get_resources_config_from_cfg(self, service):
        assert service in self.__service_configs
        return OmegaConf.to_object(self.__service_configs[service].resources)
    
    def get_all_services_from_cfg(self) -> List[str]:
        return list(self.__service_configs.keys())
    
    def get_all_frontend_endpoints_from_cfg(self) -> List[ServiceEndpointPair]:
        retval = list()
        for svc_name, ep_name in self.frontend_endpoints.items():
            retval.append(ServiceEndpointPair(svc_name, ep_name))
        return retval 


class PrometheusUtil(object):
    def __init__(self, cfg: DictConfig, logger: Logger) -> None:
        self.__host = cfg.base.prometheus.host 
        
        
    def get_deployment_cpu_usage_from_prom(self):
        pass 
        
    def get_cpu_usage_from_prom(self, 
                                dep_name: str, 
                                namespace: str,
                                start: int,
                                end: int) -> Dict | None:
        
        job = f'rate(swiftmonitor_cpu_usage{{namespace=\"train-ticket\", podname=~\"{dep_name}-.*\"}}[5s])/1000000000'
        
        resp = requests.get(
            url=self.__host + '/api/v1/query_range',
            params={
                'query': job,
                'start': f'{start}',
                'end': f'{end}',
                'step': '1s'
            }
        ) 
        
        resp = json.loads(resp.text)
                
        if resp.get('status') == 'success':
            result_type = resp.get('data').get('resultType')
            results = resp.get('data').get('result')
            
            if result_type == 'matrix':
                
                df = None 

                for result in results:
                    
                    _values = list()
                    timestamps = list()
                    
                    # metric = result.get('metric')
                    values = result.get('values')
                    
                    for el in values:
                        timestamps.append(el[0])
                        _values.append(float(el[1]))
                    
                    _df = pd.DataFrame(dict(
                        value=np.array(_values),
                        timestamps=np.array(timestamps)
                    ))
                    
                    if df is None:
                        df = _df 
                    else:
                        _tmp = pd.merge(df, _df, how='outer', on='timestamps', suffixes=['1', '2']).fillna(0)
                        df = pd.DataFrame(dict(
                            timestamps=_tmp['timestamps'],
                            value=_tmp['value1'] + _tmp['value2']
                        ))
                        
                
                return df 
            else:
                raise Exception(f'Result type is not matrix but {result_type}')
        else:
            raise Exception(f'Prometheus return status={resp.get("status")} job={job}') 


class DBUtil(object):
    def __init__(self, cfg: DictConfig, logger: Logger) -> None:
        self.__logger = logger.getChild('DBUtil')
        self.__cfg = cfg 
        
        self.__sql_driver = PostgresqlDriver(
            host=self.__cfg.base.postgresql.host,
            port=self.__cfg.base.postgresql.port,
            user=self.__cfg.base.postgresql.user,
            password=self.__cfg.base.postgresql.password,
            database=self.__cfg.base.postgresql.database
        )
    
    def get_concurrent_request_count(self, service_name, endpoint_name, limit):

        sql = f"SELECT " \
              f"    ((span_count * rt_mean) / 1000) AS value, " \
              f"    timestamp AS timestamp, " \
              f"    time AS time "  \
              f"FROM " \
              f"    span_stats " \
              f"WHERE " \
              f"    service_name = '{service_name}' " \
              f"    AND endpoint_name = '{endpoint_name}' " \
              f"    AND window_size = 1000 "  \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT " \
              f"    {limit};"
        data = self.__sql_driver.exec(sql)
        data.sort_values('time', inplace=True)
        return data.value.values, data.timestamp.values, data.time.values 

    def get_service_time(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT pt_mean,rt_mean,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.__sql_driver.exec(sql)

        return data.rt_mean.values, data.pt_mean.values
    
    def get_endpoint_execution_time_95th(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT pt_95th,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.__sql_driver.exec(sql)

        return data.pt_95th.values
    
    def get_endpoint_response_time_mean(self, service_name, endpoint_name, window_size, limit):
        sql = f"SELECT rt_mean,time  FROM response_time WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and (cluster = 'production' or cluster = 'cluster') " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.__sql_driver.exec(sql)

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
        
        data = self.__sql_driver.exec(sql)
        
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
        
        data = self.__sql_driver.exec(sql)
        
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
        
        data = self.__sql_driver.exec(sql)
        
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
              
        data = self.__sql_driver.exec(sql)
        return data.intensity.values, data.timestamp.values, data.time.values 

    def get_ac_task_max_from_src(self, src_svc, src_ep, dst_svc, dst_ep, window_size, limit) -> np.ndarray:
        sql = f"SELECT ac_task_max,time FROM response_time2 WHERE " \
              f"src_sn = '{src_svc}' and src_en = '{src_ep}' and " \
              f"dst_sn = '{dst_svc}' and dst_en = '{dst_ep}' and " \
              f"window_size = {window_size} and " \
              f"cluster = 'production' " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit}"

        data = self.__sql_driver.exec(sql)
        if len(data) == 0:
            return np.array([])

        return data.ac_task_max.values

    def get_ac_task_max(self, service_name, endpoint_name, window_size, limit) -> np.ndarray:
        sql = f"SELECT ac_task_max,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.__sql_driver.exec(sql)

        return data.ac_task_max.values

    def get_call_num(self, src_svc, src_ep, dst_svc, dst_ep):
        sql = f"SELECT request_num,time FROM response_time2 WHERE " \
              f"src_sn = '{src_svc}' and src_en = '{src_ep}' and " \
              f"dst_sn = '{dst_svc}' and dst_en = '{dst_ep}' and " \
              f"window_size = {WINDOW_SIZE_S * 1000} and " \
              f"cluster = 'production' " \
              f"ORDER BY _time DESC " \
              f"LIMIT 10"

        data = self.__sql_driver.exec(sql)

        request_num_values: np.ndarray = data.request_num.values
        return np.mean(request_num_values), np.max(request_num_values), np.min(request_num_values)

    def get_call_num2(self, service_name, endpoint_name, limit):
        sql = f"SELECT request_num,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {WINDOW_SIZE_S * 1000} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.__sql_driver.exec(sql)

        return data.request_num.values


class ServiceGraphUtil(object):
    def __init__(self, cfg: DictConfig, logger: Logger) -> None:
        self.__logger = logger.getChild('ServiceGraphUtil')
        self.__cfg = cfg 
        
        self.__sg = ServiceGraph(neo4j_url=self.__cfg.base.neo4j.url)
        self.__neo4j_project_name = self.__cfg.base.neo4j.project
    
    def get_endpoint_from_neo4j_node(self, node):
        return node['name'].split(':')[1]

    def get_service_endpoints(self, service_name) -> List[str]:
        svc_nodes = self.__sg.match_services(self.__neo4j_project_name, service_name)
        ep_nodes = self.__sg.match_endpoints(svc_nodes[0])
        return [self.get_endpoint_from_neo4j_node(i) for i in ep_nodes]

    def get_target(self, service_name, endpoint_name):
        svc_nodes = self.__sg.match_services(self.__neo4j_project_name, service_name)
        ep_nodes = self.__sg.match_endpoints(svc_nodes[0])
        this_ep_node = None
        for ep_node in ep_nodes:
            if self.get_endpoint_from_neo4j_node(ep_node) == endpoint_name:
                this_ep_node = ep_node
                break
        if this_ep_node is None:
            raise Exception(f'Endpoint {endpoint_name} do not exist in service {service_name}')
        res = list()
        downstream_ep_nodes = self.__sg.match_target_endpoints(this_ep_node)
        for node in downstream_ep_nodes:
            svc_nodes = self.__sg.match_service_by_endpoint(node)
            res.append(dict(
                service_name=svc_nodes[0]['name'],
                endpoint_name=self.get_endpoint_from_neo4j_node(node)
            ))
        return res


class Scaler(ServiceConfig, DBUtil, ServiceGraphUtil, PrometheusUtil):
    def __init__(self, cfg: DictConfig, logger: Logger):
        
        super(Scaler, self).__init__(cfg, logger)
        super(ServiceConfig, self).__init__(cfg, logger)
        super(DBUtil, self).__init__(cfg, logger)
        super(ServiceGraphUtil, self).__init__(cfg, logger)

        self.cfg = cfg 
        
        self.__logger = logger 
        
        configuration = client.Configuration()
        configuration.verify_ssl = False 
        configuration.cert_file = '/etc/kubernetes/ssl/cert/admin.pem'
        configuration.key_file = '/etc/kubernetes/ssl/key/admin-key.pem'
        configuration.host = cfg.base.kubernetes.master 
        client.Configuration.set_default(configuration)
        
        self.crd_api = client.CustomObjectsApi(client.ApiClient(pool_threads=30))
        self.k8s_apps_v1: AppsV1Api = client.AppsV1Api() 
        self.k8s_core_v1: CoreV1Api = client.CoreV1Api()
        
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
    
    def get_k8s_deployment_ready_replicas(self, dep_name: str, namespace: str):
        dep = self.get_k8s_deployment(dep_name, namespace) 
        return dep.status.ready_replicas
    
    def list_running_pods_of_dep(self, dep_name: str,
                                 namespace: str):
        dep = self.get_k8s_deployment(dep_name, namespace)
        label_selector = dep.spec.selector.match_labels
        label_selector['swiftkube.io/state'] = 'running'
        return self.list_pods(
            namespace, 
            labelselector_dict_to_str(label_selector))
    
    def list_pods_of_dep(self,
                         dep_name: str,
                         namespace: str,
                         dep=None):
        if dep is None:
            dep = self.get_k8s_deployment(dep_name, namespace)
        
        return self.list_pods(
            namespace, 
            labelselector_dict_to_str(
                dep.spec.selector.match_labels
        ))
            
    def list_pods(self,
                  namespace: str,
                  label_selector=None):
        while True:
            try:
                if label_selector is None:
                    pods = self.k8s_core_v1.list_namespaced_pod(namespace)
                else:
                    pods = self.k8s_core_v1.list_namespaced_pod(
                        namespace,
                        label_selector=label_selector)
                return pods 
            except Exception as e:
                self.__logger.error(e)
            time.sleep(0.5) 
            
    def get_pod(self, pod_name: str,
                namespace: str):
        while True:
            try:
                return self.k8s_core_v1.read_namespaced_pod(pod_name, namespace)
            except Exception as e:
                self.__logger.error(e)
            time.sleep(0.5)
            
    def _get_resources_config_from_dict(self, prev: client.V1ResourceRequirements, 
                                        resources: Dict):
        limits = None
        if 'limits' in resources:
            limits = dict()
            if 'cpu' in resources.get('limits'):
                limits['cpu'] = resources.get('limits').get('cpu')
            if 'memory' in resources.get('limits'):
                limits['memory'] = resources.get('limits').get('memory')
        requests = None 
        if 'requests' in resources:
            requests = dict()
            if 'cpu' in resources.get('requests'):
                requests['cpu'] = resources.get('requests').get('cpu')
            if 'memory' in resources.get('requests'):
                requests['memory'] = resources.get('requests').get('memory')
                
        return client.V1ResourceRequirements(claims=prev.claims, 
                                             limits=limits, 
                                             requests=requests,
                                             local_vars_configuration=prev.local_vars_configuration)
            
    def set_k8s_pod_resources(self,
                              pod_name: str,
                              namespace: str,
                              resources: List[Dict],
                              pod: client.V1Pod=None,
                              async_req: bool=False):
        # Get pod from pod name and namespace 
        # if pod is None:
        #     pod = self.get_pod(pod_name, namespace)
        
        # Patch pod 
        while True:
            
            """
            for container in pod.spec.containers:
                resources_config = None 
                for config in resources:
                    if config.name == container.name:
                        resources_config = config 
                        break 
                if resources_config is not None:
                    container.resources = \
                        self._get_resources_config_from_dict(container.resources, resources_config)
            """
            
            body = dict(
                spec=dict(
                    containers=resources
                )
            )
            
            try:
                return self.k8s_core_v1.patch_namespaced_pod(
                    pod_name, namespace, 
                    body=body, async_req=async_req)
                
            except Exception as e:
                self.__logger.error(f'set pod={pod_name} resources exception {e}')
                # pod = self.get_pod(pod_name, namespace)
            time.sleep(0.5)
    
    def set_k8s_pod_label(self, pod_name: str, 
                          namespace: str,
                          labels: Dict,
                          pod: client.V1Pod=None,
                          async_req: bool=False):
        # Get pod from pod name and namespace 
        if pod is None:
            pod = self.get_pod(pod_name, namespace)

        # Patch pod 
        while True:
            
            for key, value in labels.items():
                pod.metadata.labels[key] = value 
            
            try:
                return self.k8s_core_v1.patch_namespaced_pod(
                    pod.metadata.name, pod.metadata.namespace, 
                    body=pod, async_req=async_req)
            except Exception as e:
                self.__logger.error(f'set pod={pod_name} labels exception {e}')
                pod = self.get_pod(pod_name, namespace)
            #time.sleep(random.random() * 5)
            
    def get_k8s_deployment(self, 
                           dep_name: str, 
                           namespace: str):
        while True:
            try:
                deployment = self.k8s_apps_v1.read_namespaced_deployment(dep_name, namespace)
                return deployment
            except Exception as e:
                self.__logger.error(e)
            #time.sleep(random.random() * 5)
            
    def patch_k8s_pod(self, pod_name: str, namespace: str,
                      body: str, async_req=False):
        
        return self.k8s_core_v1.patch_namespaced_pod(
            pod_name, namespace, 
            body=body,
            async_req=async_req
        )

    def get_k8s_deployment_replicas(self, dep_name: str, namespace: str):
        return self.get_k8s_deployment(dep_name, namespace).spec.replicas 

    def set_k8s_deployment_replicas(self, 
                                    dep_name: str, 
                                    namespace: str, 
                                    replicas: int):

        while True:
            try:
                resp = self.k8s_apps_v1.read_namespaced_deployment(
                    dep_name, namespace) 
                break 
            except Exception as e:
                self.__logger.error(e)
            time.sleep(random.random() * 5)
            
        deployment = resp
        if deployment.spec.replicas == replicas:
            return deployment  
                
        while True:
            try:
                deployment.spec.replicas = replicas
                return self.k8s_apps_v1.patch_namespaced_deployment(
                    dep_name, namespace,
                    body=deployment
                )
            except:
                self.__logger.error(f'update deployment={dep_name} exception')
                deployment = self.k8s_apps_v1.read_namespaced_deployment(dep_name, namespace)
            #time.sleep(random.random() * 5)
            
    