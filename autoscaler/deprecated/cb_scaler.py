import time 
from logging import Logger 

import asyncio 

from .. import Scaler 

SWIFTKUBE_STATE_LABEL = 'swiftkube.io/state'
SWIFTKUBE_STATE_RUNNING = 'running'
SWIFTKUBE_STATE_SLEEPING = 'sleeping'

INIT_LABELS = {
    SWIFTKUBE_STATE_LABEL: SWIFTKUBE_STATE_RUNNING
}


class ContextualBanditScaler(Scaler):
    def __init__(self, cfg, logger: Logger):
        super().__init__(cfg, logger) 
        
    def start(self):
        # Init Swiftkube labels 
        init_label_start = time.time()
        asyncio.run(self.init_pod_labels())
        init_label_time = time.time() - init_label_start
        self.logger.info(f'Init pod labels use {init_label_time:.2f} seconds.')
        
        # Init thresholds 
        self.thresholds = dict()
        services = self.get_all_services_from_cfg()
        for service in services:
            self.thresholds[service] = 1.0
        
        # Init replicas 
        self.replicas = dict()
        for service in services:
            self.replicas[service] = dict(
                replicas=self.get_service_max_replicas(service),
                running_replicas=self.get_service_max_replicas(service)
            )
        
        # Sync replcas 
        asyncio.run(self.sync_replicas())
            
    async def _sync_replicas(self, service, replicas_conf):
        replicas = replicas_conf.get('replicas') 
        running_replicas = replicas_conf.get('running_replicas')
        assert running_replicas <= replicas
        
        dep_name = self.get_k8s_dep_name_from_cfg(service)
        namespace = self.get_k8s_namespace_from_cfg()
        
        # Set replicas field of Deployment 
        _ = self.set_k8s_deployment_replicas(dep_name, namespace, replicas)
        
        # Wait
        while(not self.is_k8s_deployment_replicas_all_available(dep_name, namespace)):
            time.sleep(0.5)
            
        # Set running replicas 
        pods = self.list_pods_of_dep(dep_name, namespace)
        new_pods, running_pods, sleeping_pods = list(), list(), list()
        for pod in pods.items:
            # New pod 
            if SWIFTKUBE_STATE_LABEL not in pod.metadata.labels:
                new_pods.append(pod)
            # Running pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_RUNNING:
                running_pods.append(pod)
            # Sleeping pod 
            elif pod.metadata.labels[SWIFTKUBE_STATE_LABEL] == SWIFTKUBE_STATE_SLEEPING:
                sleeping_pods.append(pod)
        if len(running_pods) > running_replicas:
            delta_to_sleep = len(running_pods) - running_replicas 
        elif len(running_pods) < running_replicas:
            pass  
            
    async def sync_replicas(self):
        tasks = list()
        for service, replicas_conf in self.replicas.items():
            tasks.append((
                service, 
                asyncio.create_task(self._sync_replicas(service, replicas_conf))
            ))
        for task in tasks:
            await task[1]
        
    async def _init_pod_labels(self, k8s_dep_name: str):
        pods = self.list_pods_of_dep(
            k8s_dep_name,
            self.get_k8s_namespace_from_cfg()
        )
        f_retvals = list()
        retvals = list()
        for _, pod in enumerate(pods.items):
            f_retvals.append(self.set_k8s_pod_label(
                pod.metadata.name, pod.metadata.namespace,
                INIT_LABELS, pod, async_req=True))
            
        for f_retval in f_retvals:
            retvals.append(f_retval.get())
        
        return retvals 
    
    async def init_pod_labels(self):
        services = self.get_all_services_from_cfg()  
        tasks = list()
        for service in services:
            k8s_dep_name = self.get_k8s_dep_name_from_cfg(service)
            tasks.append((service, asyncio.create_task(self._init_pod_labels(k8s_dep_name))))
        for task in tasks:
            self.logger.info(f'{len(await task[1])} pods initialized in service {task[0]}') 

