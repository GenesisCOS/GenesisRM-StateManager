import sys
import time
import pathlib 
import threading
import logging

import hydra 
from omegaconf import DictConfig

from .util.locust import Locust 

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
    logger.info(banner)
    logger.info(f'services = {cfg.service_config.name}')
    
    scaler = None 
    
    if cfg.autoscaler.name != 'none':
        # Run scaler 
        if cfg.autoscaler.name == 'swift':
            from .statecontroller.swift_scaler import SwiftKubeScaler
            scaler = SwiftKubeScaler(cfg, pathlib.Path('autoscaler/data/swiftkube_data'), 
                                     logger.getChild('SwiftKube'))
            
        elif cfg.autoscaler.name == 'nw_scaler':
            from .statecontroller.nw_scaler import NWScaler
            scaler = NWScaler(cfg, logger.getChild('NWScaler'))
            
        elif cfg.autoscaler.name == 'ahpa_scaler':
            from .statecontroller.ahpa_scaler import AHPAScaler
            scaler = AHPAScaler(cfg, logger.getChild('AHPAScaler')) 
        
        elif cfg.autoscaler.name == 'mem_scaler':
            from .statecontroller.mem_scaler import MemScaler 
            scaler = MemScaler(cfg, logger.getChild('MemScaler'))
            
        elif cfg.autoscaler.name == 'autothreshold':
            from .statecontroller.autothreshold import AutoThreshold
            scaler = AutoThreshold(cfg, pathlib.Path('autoscaler/data/autothreshold_data'), 
                              logger.getChild('AutoThreshold'))
            
        elif cfg.autoscaler.name == 'autoweight':
            from .statecontroller.autoweight import AutoWeight
            scaler = AutoWeight(cfg, pathlib.Path('autoscaler/data/autoweight_data'),
                                logger.getChild('AutoWeight'))
        
        else:
            raise Exception(f'unsupported autoscaler {cfg.autoscaler.name}')
    
    # Pre start 
    if scaler is not None:
        ok = scaler.pre_start()
        if not ok:
            raise Exception('scaler预启动失败')
    
    locust = Locust(
        cfg, f'csv-output-{int(time.time())}', 
        cfg.locust.workload, logger.getChild('Locust')
    )
    
    locust_run = False 
    if 'locust_enabled' not in cfg.autoscaler or \
            cfg.autoscaler.locust_enabled:
        locust.start()
        locust_run = True 
    else:
        logger.info('Locust will not run ...')
        
    scaler_thread = None 
    if scaler is not None:
        scaler_thread = threading.Thread(target=scaler.start, name='scaler-thread', daemon=True)
        scaler_thread.start()
        scaler_thread.join()

    if locust_run:
        locust.wait()


if __name__ == '__main__':
    sys.exit(main())
