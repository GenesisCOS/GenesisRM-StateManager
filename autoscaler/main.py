import sys
import os 
import copy 
import time
import subprocess
import threading
import logging
import pathlib 

import hydra 
from omegaconf import DictConfig

def with_locust(temp_dir, locustfile, url, workers, dataset):
    
    env = copy.deepcopy(os.environ)
    
    env['DATASET'] = dataset 
    env['HOST'] = url

    args = [
        'locust',
        '--worker',
        '-f', locustfile,
    ]
    worker_ps = []
    for _ in range(workers):
        worker_ps.append(subprocess.Popen(
            args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env 
        ))

    args = [
        'locust',
        '--master',
        '--expect-workers', f'{workers}',
        '--headless',
        '-f', locustfile,
        '-H', url,
        '--csv', temp_dir/'locust',
        '--csv-full-history',
    ]
    master_p = subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL,
        env=env
    )

    time.sleep(1)
    return master_p, worker_ps

@hydra.main(version_base=None, config_path='../config', config_name='config')
def main(cfg: DictConfig) -> None:
    
    logger = logging.getLogger('Main')
    enabled_service_config = cfg.enabled_service_config
    logger.info(f'enabled service config = {enabled_service_config}')
    
    scaler = None 
    
    if cfg.scaler.enabled_scaler != 'none':
        # Run scaler 
        if cfg.scaler.enabled_scaler == 'swiftkube_scaler':
            from .swiftkube_scaler import SwiftKubeScaler
            scaler = SwiftKubeScaler(cfg, logger.getChild('SwiftKubeScaler'))
            
        elif cfg.scaler.enabled_scaler == 'cb_scaler':
            from .cb_scaler import ContextualBanditScaler
            scaler = ContextualBanditScaler(cfg, logger.getChild('ContextualBanditScaler'))
            
        elif cfg.scaler.enabled_scaler == 'nw_scaler':
            from .nw_scaler import NWScaler
            scaler = NWScaler(cfg, logger.getChild('NWScaler'))
            
        elif cfg.scaler.enabled_scaler == 'ahpa_scaler':
            from .ahpa_scaler import AHPAScaler
            scaler = AHPAScaler(cfg, logger.getChild('AHPAScaler')) 
        
        elif cfg.scaler.enabled_scaler == 'mem_scaler':
            from .mem_scaler import MemScaler 
            scaler = MemScaler(cfg, logger.getChild('MemScaler'))
        
        elif cfg.scaler.enabled_scaler == 'k8s_vpa_scaler':
            from .k8s_vpa_scaler import K8sVPAScaler
            scaler = K8sVPAScaler(cfg, logger.getChild('K8sVPAScaler'))
        
        else:
            raise Exception(f'unsupported autoscaler {cfg.scaler.enabled_scaler}')
        
    if scaler is not None:
        scaler.pre_start()
    
    # Run locust 
    workers = cfg.base.locust.workers 
    locustfile = pathlib.Path(f'autoscaler/locust/{enabled_service_config}')/'locustfile.py'
    temp_dir = pathlib.Path('autoscaler/locust/output')/f'csv-output-{int(time.time())}'
    dataset = f'autoscaler/data/datasets/{enabled_service_config}/rps/{cfg.base.locust.workload}.txt'
    url = cfg.base.locust.url 
    
    locust_run = False 
    if 'locust_enabled' not in cfg.scaler[cfg.scaler.enabled_scaler] or \
            cfg.scaler[cfg.scaler.enabled_scaler]['locust_enabled']:
        master_p, worker_ps = with_locust(temp_dir, locustfile, url, workers, dataset)
        locust_run = True 
    else:
        logger.info('Locust will not run ...')

    if locust_run:
        with master_p:
            
            scaler_thread = None 
            if scaler is not None:
                scaler_thread = threading.Thread(target=scaler.start, name='scaler-thread', daemon=True)
                scaler_thread.start()
            
            # Wait locust 
            while True:
                if master_p.poll() is not None:
                    break 
                time.sleep(1)
    else:
        scaler_thread = None 
        if scaler is not None:
            scaler_thread = threading.Thread(target=scaler.start, name='scaler-thread', daemon=True)
            scaler_thread.start()
        scaler_thread.join()
    
    if locust_run:
        for p in worker_ps:
            p.wait()


if __name__ == '__main__':
    sys.exit(main())
