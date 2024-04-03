import time 

from logging import Logger
from . import Scaler 


class NWScaler(Scaler):
    def __init__(self, cfg, logger: Logger):
        super().__init__(cfg, logger)
        
        self.k8s_namespace = self.get_k8s_namespace_from_cfg()
        self.author = cfg.scaler.nw_scaler.author 
        
    def start(self):
        self.logger.info('Startup ...')
        services = self.get_all_services_from_cfg()
        for service in services:
            dep_name = self.get_k8s_dep_name_from_cfg(service)
            replicas = self.get_k8s_deployment_replicas(
                dep_name, self.k8s_namespace
            )
            self.logger.info(f'service {service} has {replicas} instances.')
        while True:
            self.logger.info(f'Running ... {self.author}')
            time.sleep(1)
            
