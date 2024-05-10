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

ROOT_PATH = os.path.split(os.path.realpath(__file__))[0]

def with_locust(temp_dir, locustfile, url, workers, dataset, logger):
    
    env = copy.deepcopy(os.environ)
    
    # Run opentelemetry collector 
    logger.info('Start Opentelemetry Collector ...')
    args = [
        'otelcol',
        f'--config={ROOT_PATH}/../config/otelcol/config.yaml'
    ]
    otelcol_p = subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )
    
    env['DATASET'] = dataset 
    env['HOST'] = url

    # Run locust workers 
    logger.info('Start Locust workers ...')
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

    # Run locust master
    logger.info('Start Locust master ...')
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
    return master_p, worker_ps, otelcol_p

banner = '''

   ______                     _      ____  __  ___
  / ____/__  ____  ___  _____(_)____/ __ \/  |/  /
 / / __/ _ \/ __ \/ _ \/ ___/ / ___/ /_/ / /|_/ / 
/ /_/ /  __/ / / /  __(__  ) (__  ) _, _/ /  / /  
\____/\___/_/ /_/\___/____/_/____/_/ |_/_/  /_/   
                                                 
                                           -- v1.0.0
                                    
'''

@hydra.main(version_base=None, config_path='../config', config_name='config')
def main(cfg: DictConfig) -> None:
    
    logger = logging.getLogger('Main')
    enabled_service_config = cfg.enabled_service_config
    logger.info(f'enabled service config = {enabled_service_config}')
    
    logger.info(banner)
    
    scaler = None 
    
    if cfg.scaler.enabled_scaler != 'none':
        # Run scaler 
        if cfg.scaler.enabled_scaler == 'swiftkube_scaler':
            from .statecontroller.swift_scaler import SwiftKubeScaler
            scaler = SwiftKubeScaler(cfg, logger.getChild('SwiftKubeScaler'))
            
        elif cfg.scaler.enabled_scaler == 'nw_scaler':
            from .statecontroller.nw_scaler import NWScaler
            scaler = NWScaler(cfg, logger.getChild('NWScaler'))
            
        elif cfg.scaler.enabled_scaler == 'ahpa_scaler':
            from .statecontroller.ahpa_scaler import AHPAScaler
            scaler = AHPAScaler(cfg, logger.getChild('AHPAScaler')) 
        
        elif cfg.scaler.enabled_scaler == 'mem_scaler':
            from .statecontroller.mem_scaler import MemScaler 
            scaler = MemScaler(cfg, logger.getChild('MemScaler'))
        
        else:
            raise Exception(f'unsupported autoscaler {cfg.scaler.enabled_scaler}')
    
    # Pre start 
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
        master_p, worker_ps, otelcol_p = with_locust(
            temp_dir, locustfile, url, workers, dataset, logger)
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
        otelcol_p.kill()
        otelcol_p.wait()


if __name__ == '__main__':
    sys.exit(main())
