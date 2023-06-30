import math
import time
import threading
from typing import List, Dict, Any

import grpc
import numpy as np

from .sttf.estimator import Estimator as SttfEstimator
from .data import ServiceEndpointPair
# from proto import elastic_deployment_pb2 as ed_pb
# from proto import elastic_deployment_pb2_grpc as ed_grpc
from util.postgresql import PostgresqlDriver
from servicegraph import ServiceGraph

WINDOW_SIZE_S = 5
DEFAULT_MAX_THREAD = 20
DEFAULT_MAX_REPLICAS = 10
cache = dict()

L1_OFFSET = 5


class Scaler(object):
    def __init__(self, args, service_configs: Dict):
        self.args = args

        self.response_time_table = args.response_time_table
        self.task_stats_table2 = args.task_stats_table2
        self.task_stats_table = args.task_stats_table
        self.request_num_table2 = args.request_num_table2
        self.request_num_table = args.request_num_table

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

    def get_running_replicas(self, service_name: str):
        with grpc.insecure_channel(self.args.ed_host) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.GetRunningPodReplicas(ed_pb.Deployment(
                name=service_name,
                namespace=self.args.k8s_namespace
            ))
        return resp.replicas

    def get_replicas(self, service_name: str):
        with grpc.insecure_channel(self.args.ed_host) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.GetPodReplicas(ed_pb.Deployment(
                name=service_name,
                namespace=self.args.k8s_namespace
            ))
        return resp.replicas

    def set_running_replicas(self, service_name: str, replicas: int):
        with grpc.insecure_channel(self.args.ed_host) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            stub.SetRunningPodReplicas(ed_pb.DeploymentReplicasOpsRequest(
                target_replicas=replicas,
                target=ed_pb.Deployment(name=service_name, namespace=self.args.k8s_namespace)
            ))

    def set_replicas(self, service_name: str, replicas: int):
        with grpc.insecure_channel(self.args.ed_host) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            stub.SetPodReplicas(ed_pb.DeploymentReplicasOpsRequest(
                target_replicas=replicas,
                target=ed_pb.Deployment(name=service_name, namespace=self.args.k8s_namespace)
            ))

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
        sql = f"SELECT pt_mean,rt_mean,time  FROM {self.response_time_table} WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.rt_mean.values, data.pt_mean.values

    def get_ac_task_max2(self, src_svc, src_ep, dst_svc, dst_ep, window_size, limit) -> np.ndarray:
        sql = f"SELECT ac_task_max,time FROM {self.task_stats_table2} WHERE " \
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
        sql = f"SELECT ac_task_max,time  FROM {self.task_stats_table} WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {window_size} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.ac_task_max.values

    def get_call_num(self, src_svc, src_ep, dst_svc, dst_ep):
        sql = f"SELECT request_num,time FROM {self.request_num_table} WHERE " \
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
        sql = f"SELECT request_num,time  FROM {self.request_num_table2} WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = {WINDOW_SIZE_S * 1000} " \
              f"ORDER BY _time DESC " \
              f"LIMIT {limit};"

        data = self.sql_driver.exec(sql)

        return data.request_num.values


class MicroKubeScaler(Scaler):
    def __init__(self, args, root_endpoints: List[ServiceEndpointPair], service_configs: Dict,
                 sttf_estimator: SttfEstimator | None, lttf_estimator: Any | None):

        super().__init__(args, service_configs)
        if sttf_estimator is not None:
            print('use short term time-series estimator')
        self.sttf_estimator = sttf_estimator

        if lttf_estimator is not None:
            print('use long term time-series estimator')
        self.lttf_estimator = lttf_estimator

        self.root_endpoints = root_endpoints
        self.service_configs = service_configs

        self.l1_sync_period_sec = args.l1_sync_period_sec
        self.l2_sync_period_sec = args.l2_sync_period_sec

        self.args = args

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

        self.response_time_table = args.response_time_table
        self.task_stats_table2 = args.task_stats_table2
        self.task_stats_table = args.task_stats_table
        self.request_num_table2 = args.request_num_table2
        self.request_num_table = args.request_num_table

    def start(self):
        print('Scaler starting ...')
        l2_start_time = time.time()
        cur_time = time.time()
        while True:
            start_time = time.time()

            g_res: Dict[str, int] = dict()
            results: Dict[str, Dict[str, int]] = dict()
            results_lock = threading.Lock()

            threads = list()
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

            print('l1 scaling parallelism needed result:')
            for service_name in g_res:
                print(f'\t{service_name} need {format(g_res[service_name], ".2f")} parallelism')

            self.do_l1_scaling(g_res)

            end_time = time.time()
            cur_time = end_time
            if end_time - start_time < self.l1_sync_period_sec:
                time.sleep(self.l1_sync_period_sec - (end_time - start_time))

    def do_l1_scaling(self, results: Dict[str, int]):
        for service_name in results:
            max_thread = self.get_service_max_thread(service_name)
            all_replicas = self.get_replicas(service_name)

            thread_need = results[service_name]
            running_replicas_need = math.ceil(thread_need / max_thread)

            replicas_need = all_replicas
            if all_replicas < running_replicas_need + 3:
                replicas_need = running_replicas_need + 3
            replicas_need = min(self.get_service_max_replicas(service_name), replicas_need)

            # TODO
            # elif all_replicas > running_replicas_need + 6:
            #     replicas_need = running_replicas_need + 6

            print(f'(MicroKube L1 Autoscaler) ==> {service_name} need {running_replicas_need} running replicas.')
            print(f'(MicroKube L1 Autoscaler) ==> {service_name} need {replicas_need} replicas.')

            self.set_running_replicas(service_name, running_replicas_need)
            self.set_replicas(service_name, replicas_need)

    def start_l1_scaling(self, service_name: str, endpoint_name: str, results, results_lock):
        _, upper_bound, _, last_value = self.sttf_estimator.predict(service_name, endpoint_name)

        key = f'{service_name}:{endpoint_name}'
        # thread_need = max(last_value, self.prev_sttf_upper_bound.get(key, 0))
        thread_need = max(last_value, upper_bound)
        self.prev_sttf_upper_bound[key] = upper_bound

        res = dict()

        t0 = time.time()
        thread_need += L1_OFFSET
        self.calculate_parallelism(service_name, endpoint_name, thread_need, res)
        print(f'l1 scaling use {time.time() - t0} seconds calculate concurrency')

        results_lock.acquire()
        key = f'l1-{service_name}:{endpoint_name}'
        results[key] = res
        results_lock.release()

    def calculate_parallelism(self, service_name, endpoint_name, thread_need, res):
        res[service_name] = res.get(service_name, 0) + thread_need

        downstream = self.get_downstream(service_name, endpoint_name)
        for downstream_service in downstream:
            d_service_name = downstream_service.get('service_name')
            d_endpoint_name = downstream_service.get('endpoint_name')

            cache_key = f'l1-{service_name}:{endpoint_name}=>{d_service_name}:{d_endpoint_name}'

            if cache_key in cache and time.time() - cache.get(cache_key).get('last_sync_time') <= 10 * 60:
                _thread_need = thread_need * cache.get(cache_key).get('value')
            else:
                ac_task_max2 = self.get_ac_task_max2(service_name, endpoint_name, d_service_name, d_endpoint_name,
                                                     window_size=60000, limit=2)
                if len(ac_task_max2) == 0:
                    continue
                ac_task_max = self.get_ac_task_max(service_name, endpoint_name, window_size=60000, limit=2)
                weight = min(1., float(np.mean(ac_task_max2 / ac_task_max)))

                _thread_need = thread_need * weight
                cache[cache_key] = dict(
                    value=weight,
                    last_sync_time=time.time()
                )

            print(cache_key, f'weight: {cache.get(cache_key).get("value")}')

            self.calculate_parallelism(d_service_name, d_endpoint_name, _thread_need, res)


class AIMDHScaler(Scaler):
    def __init__(self, args, service_configs: Dict):

        super().__init__(args, service_configs)
        self.args = args
        self.sg = ServiceGraph(neo4j_url=args.neo4j_url)

        from kubernetes import client, config

        config.load_kube_config(config_file='E:\\Projects\\microkube-python-master\\microkube-python-master\\config\\kubernetes\\config')
        self.k8s_apps_v1 = client.AppsV1Api()

        self.a = 1
        self.b = 2

        pg_param = dict(
            host="222.201.144.196",
            port=5432,
            database="trainticket",
            user="postgres",
            password="Admin@123_."
        )

        self.locust_sql_driver = PostgresqlDriver(**pg_param)

    def get_endpoint_rt(self, url, method, limit):
        sql = f"SELECT response_time  FROM locust WHERE " \
              f"url = '{url}' and method = '{method}' " \
              f"ORDER BY start_time DESC " \
              f"LIMIT {limit};"

        data = self.locust_sql_driver.exec(sql)

        return data.response_time.values

    def start(self):
        print('Start AIMD-H Autoscaler.')
        services = self.sg.match_services(self.args.neo4j_project_name)
        threads = list()

        target_svc = None
        for service in services:
            if service.get('name') == 'travel-plan-service':
                target_svc = service
                break

        for svc in services:
            t = threading.Thread(target=self.do_scaling,
                                 args=(svc, target_svc),
                                 daemon=True)
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()

    def do_scaling(self, service, target):
        endpoints = self.sg.match_endpoints(target)
        max_replicas = self.get_service_max_replicas(service.get('name'))

        while True:
            queueing = False
            ok = False
            for i in endpoints:
                locust_rt = self.get_endpoint_rt('/wrk2-api/review/compose', 'POST', 10)
                rt_mean = np.mean(locust_rt)
                # rt_mean_values, pt_mean_values = self.get_service_time(target.get('name'), i.get('uri'), 5000, 1)
                # if len(rt_mean_values) == 0 or len(pt_mean_values) == 0:
                #     continue
                # if rt_mean_values[0] > self.get_service_aimd_threshold(target.get('name')):
                if rt_mean > 2 * 1000000000:
                    ok = True
                    queueing = True
                    break
                ok = True

            k8s_name = self.get_k8s_name(service.get('name'))
            deployment = self.k8s_apps_v1.read_namespaced_deployment(
                name=k8s_name, namespace=self.args.k8s_namespace)
            cur_replicas = deployment.spec.replicas

            increase = False
            if ok:
                if not queueing:
                    target_replicas = max(1, cur_replicas - self.a)
                    print(f'[{service.get("name")}] decrease {self.a} replicas.')
                else:
                    target_replicas = min(max_replicas, cur_replicas * self.b)
                    increase = True
                    print(f'[{service.get("name")}] increase {self.b * cur_replicas - cur_replicas} replicas.')
            else:
                target_replicas = -1
                print(f'[{service.get("name")}] nothing done.')

            if target_replicas > 0:
                while True:
                    try:
                        deployment = self.k8s_apps_v1.read_namespaced_deployment(
                            name=k8s_name, namespace=self.args.k8s_namespace)
                        deployment.spec.replicas = target_replicas
                        _ = self.k8s_apps_v1.patch_namespaced_deployment(
                            name=k8s_name, namespace=self.args.k8s_namespace,
                            body=deployment
                        )
                        break
                    except:
                        print(f'update {service.get("name")} exception')
                    time.sleep(0.5)

            if increase:
                time.sleep(70)
            else:
                time.sleep(20)

class AIMDVScaler(Scaler):
    def __init__(self, args, service_configs: Dict):
        super().__init__(args, service_configs)
        self.args = args
        self.sg = ServiceGraph(neo4j_url=args.neo4j_url)

        from kubernetes import client, config

        config.load_kube_config()
        self.k8s_apps_v1 = client.AppsV1Api()

        self.min_cpu_per_container = 300
        self.max_cpu_per_container = 1000
        self.cpu_step = 40
        self.b = 1.5

    def start(self):
        print('Start AIMD-V Autoscaler.')
        services = self.sg.match_services(self.args.neo4j_project_name)
        threads = list()

        target_svc = None
        for service in services:
            if service.get('name') == 'travel-plan-service':
                target_svc = service
                break

        for svc in services:
            t = threading.Thread(target=self.do_scaling,
                                 args=(svc, target_svc),
                                 daemon=True)
            threads.append(t)
            t.start()

        for thread in threads:
            thread.join()

    def do_scaling(self, service, target):
        endpoints = self.sg.match_endpoints(target)
        max_replicas = self.get_service_max_replicas(service.get('name'))

        while True:
            queueing = False
            ok = False
            for i in endpoints:
                rt_mean_values, pt_mean_values = self.get_service_time(target.get('name'), i.get('uri'), 5000, 1)
                if len(rt_mean_values) == 0 or len(pt_mean_values) == 0:
                    continue
                if rt_mean_values[0] > self.get_service_aimd_threshold(target.get('name')):
                    ok = True
                    queueing = True
                    break
                ok = True

            deployment = self.k8s_apps_v1.read_namespaced_deployment(name=service.get('name'), namespace=self.args.k8s_namespace)
            cur_replicas = deployment.spec.replicas

            container = None
            for c in deployment.spec.template.spec.containers:
                if c.name == service.get('name'):
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

            increase = False
            if ok:
                if not queueing:
                    target_cpu_limit = max(self.min_cpu_per_container, cur_all_cpu - self.cpu_step)
                    print(f'[{service.get("name")}] decrease {self.cpu_step} cpu allocation.')
                else:
                    target_cpu_limit = min(max_replicas * self.max_cpu_per_container, math.ceil(cur_all_cpu * self.b))
                    print(f'[{service.get("name")}] increase {target_cpu_limit - cur_all_cpu} cpu allocation.')
            else:
                target_cpu_limit = -1
                print(f'[{service.get("name")}] nothing done.')

            if target_cpu_limit > 0:
                while True:
                    try:
                        per_container_cpu = math.ceil(target_cpu_limit / cur_replicas)
                        if per_container_cpu < self.min_cpu_per_container:
                            target_replicas = math.ceil(target_cpu_limit / self.min_cpu_per_container)
                            target_cpu_limit = self.min_cpu_per_container
                        elif per_container_cpu > self.max_cpu_per_container:
                            target_replicas = math.ceil(target_cpu_limit / self.max_cpu_per_container)
                            target_cpu_limit = self.max_cpu_per_container
                        else:
                            target_replicas = cur_replicas
                            target_cpu_limit = per_container_cpu

                        target_replicas = min(max_replicas, target_replicas)
                        target_replicas = max(1, target_replicas)
                        if target_replicas > cur_replicas:
                            increase = True
                        deployment = self.k8s_apps_v1.read_namespaced_deployment(name=service.get('name'), namespace=self.args.k8s_namespace)
                        for container in deployment.spec.template.spec.containers:
                            if container.name == service.get('name'):
                                print(f'service {service.get("name")} cpu -> {target_cpu_limit}m')
                                container.resources.limits = {
                                    'cpu': f'{target_cpu_limit}m'
                                }
                                break
                        deployment.spec.replicas = target_replicas
                        _ = self.k8s_apps_v1.patch_namespaced_deployment(
                            name=service.get('name'), namespace=self.args.k8s_namespace,
                            body=deployment
                        )
                        break
                    except Exception as e:
                        print(f'update {service.get("name")} exception: {e}')
                    time.sleep(0.5)

            if increase:
                time.sleep(70)
            else:
                time.sleep(10)
