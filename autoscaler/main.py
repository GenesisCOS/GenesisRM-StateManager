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
    enabled_service_config = cfg.enabled_service_config
    logger.info(f'enabled service config = {enabled_service_config}')
    
    logger.info(banner)
    
    scaler = None 
    
    if cfg.scaler.enabled_scaler != 'none':
        # Run scaler 
        if cfg.scaler.enabled_scaler == 'swiftkube_scaler':
            from .statecontroller.swift_scaler import SwiftKubeScaler
            scaler = SwiftKubeScaler(cfg, pathlib.Path('autoscaler/data/swiftkube_data'), 
                                     logger.getChild('SwiftKube'))
            
        elif cfg.scaler.enabled_scaler == 'nw_scaler':
            from .statecontroller.nw_scaler import NWScaler
            scaler = NWScaler(cfg, logger.getChild('NWScaler'))
            
        elif cfg.scaler.enabled_scaler == 'ahpa_scaler':
            from .statecontroller.ahpa_scaler import AHPAScaler
            scaler = AHPAScaler(cfg, logger.getChild('AHPAScaler')) 
        
        elif cfg.scaler.enabled_scaler == 'mem_scaler':
            from .statecontroller.mem_scaler import MemScaler 
            scaler = MemScaler(cfg, logger.getChild('MemScaler'))
            
        elif cfg.scaler.enabled_scaler == 'cb_scaler':
            from .statecontroller.autothreshold import AutoThreshold
            scaler = AutoThreshold(cfg, pathlib.Path('autoscaler/data/autothreshold_data'), 
                              logger.getChild('AutoThreshold'))
            
        elif cfg.scaler.enabled_scaler == 'autoweight':
            from .statecontroller.autoweight import AutoWeight
            scaler = AutoWeight(cfg, pathlib.Path('autoscaler/data/autoweight_data'),
                                logger.getChild('AutoWeight'))
        
        else:
            raise Exception(f'unsupported autoscaler {cfg.scaler.enabled_scaler}')
    
    # Pre start 
    if scaler is not None:
        ok = scaler.pre_start()
        if not ok:
            raise Exception('scaler预启动失败')
    
    locust = Locust(
        cfg, f'csv-output-{int(time.time())}', 
        cfg.base.locust.workload, logger.getChild('Locust')
    )
    
    locust_run = False 
    if 'locust_enabled' not in cfg.scaler[cfg.scaler.enabled_scaler] or \
            cfg.scaler[cfg.scaler.enabled_scaler]['locust_enabled']:
        locust.start()
        locust_run = True 
    else:
        logger.info('Locust will not run ...')

    if locust_run:
        scaler_thread = None 
        if scaler is not None:
            scaler_thread = threading.Thread(target=scaler.start, name='scaler-thread', daemon=True)
            scaler_thread.start()
            
        locust.wait()
    else:
        scaler_thread = None 
        if scaler is not None:
            scaler_thread = threading.Thread(target=scaler.start, name='scaler-thread', daemon=True)
            scaler_thread.start()
        scaler_thread.join()
    
    locust.stop()


if __name__ == '__main__':
    sys.exit(main())
