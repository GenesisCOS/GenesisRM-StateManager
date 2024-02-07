import time 
from concurrent import futures
from concurrent.futures import as_completed
from logging import Logger
 
from . import Scaler 


class K8sVPAScaler(Scaler):
    def __init__(self, cfg, logger: Logger):
        super().__init__(cfg, logger) 
        self.__logger = logger 
    
    def pre_start(self):
        self.__logger.info('K8s VPA pre-start') 
        
    def start(self):
        self.__logger.info('K8s VPA start')
        services = self.get_all_services_from_cfg()
        while True:
            __cl_start = time.time()
            cpu_tgt_map = dict()
            
            # Get recommandation from VPA object 
            for service in services:
                cpu_tgt = self.__get_vpa_cpu_target(service)
                cpu_tgt_map[service] = cpu_tgt
            
            # Update pods 
            with futures.ThreadPoolExecutor(max_workers=80) as executor:
                ret_futures = list()
                for service, cpu_tgt in cpu_tgt_map.items():
                    #limit = f'{int(cpu_tgt / 0.6)}m'
                    limit = '3000m'
                    request = f'{int(cpu_tgt)}m'
                    future = executor.submit(self.__set_pod_cpu_resource, service, request, limit)
                    ret_futures.append(future)
                for future in as_completed(ret_futures):
                    future.result()
            self.__logger.info(f'Control loop use {time.time() - __cl_start}s')
            time.sleep(1)
            
    def __set_pod_cpu_resource(self, service, request: str, limit: str):
        __l = self.__logger.getChild('Operation')
        dep_name = self.get_k8s_dep_name_from_cfg(service)
        pods = self.list_pods_of_dep(dep_name, self.get_k8s_namespace_from_cfg())
        for pod in pods.items:
            try:
                pod = self.patch_k8s_pod(
                    pod.metadata.name, pod.metadata.namespace,
                    body={
                        'spec': {
                            'containers': [{
                                    'name': service,
                                    'resources': {
                                        'limits': { 'cpu': limit },
                                        'requests': { 'cpu': request }
                                    }
                                }]
                        }
                    },
                    async_req=True
                )
            except Exception as e:
                __l.info(f'set pod (name={pod.metadata.name} namespace={pod.metadata.namespace}) failed: {e}.')  
        
    def __get_vpa_cpu_target(self, service):
        vpa_name = service[:-8] + '-vpa'  # -service 
        vpa_obj = self.crd_api.get_namespaced_custom_object(
            group='autoscaling.k8s.io',
            version='v1',
            plural='verticalpodautoscalers',
            name=vpa_name,
            namespace=self.get_k8s_namespace_from_cfg()
        )
        for r in vpa_obj['status']['recommendation']['containerRecommendations']:
            if r['containerName'] == service:
                cpu_tgt = r['target']['cpu']
                if cpu_tgt.endswith('m'):
                    return int(cpu_tgt[:-1])
                else:
                    return int(cpu_tgt) * 1000
 
