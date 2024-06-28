import os 
import copy 
import time 
import pathlib 
import subprocess
from logging import Logger

import pandas as pd 
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler 


def with_locust(output_dir,
                dataset, 
                cfg, 
                logger: Logger):
    
    script_dir_path = os.path.split(os.path.realpath(__file__))[0]
    locustfile = str(pathlib.Path(cfg.service_config.locust.script))
    url = cfg.service_config.locust.host 
    service_name = cfg.service_config.locust.service_name 
    workers = cfg.locust.workers 
    dataset_dir = pathlib.Path(cfg.locust.workload_dir)
    use_timeout = cfg.locust.use_timeout 
    timeout_sec = cfg.locust.timeout_sec
    dataset = dataset_dir / dataset 
    
    env = copy.deepcopy(os.environ)
    
    # Run opentelemetry collector 
    logger.info('启动 otelcol ...')
    args = [
        'otelcol',
        f'--config={script_dir_path}/../../config/otelcol/config.yaml'
    ]
    otelcol_p = subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )
    
    if use_timeout:
        env['TIMEOUT'] = str(timeout_sec)
    env['DATASET'] = str(dataset)
    env['HOST'] = url
    env['SERVICE_NAME'] = str(service_name)
    env['CLUSTER_GROUP'] = str(cfg.service_config.locust.cluster_group)
    env['DATA_DIR'] = str(output_dir)
    if 'locust' in cfg.autoscaler:
        if 'report_interval' in cfg.autoscaler.locust:
            env['REPORT_INTERVAL'] = str(cfg.autoscaler.locust.report_interval)
        if 'report_offset' in cfg.autoscaler.locust:
            env['REPORT_OFFSET'] = str(cfg.autoscaler.locust.report_offset)

    # Run locust workers 
    args = [
        'locust',
        '--worker',
        '--master-port', f'{cfg.service_config.locust.master_port}',
        '-f', locustfile,
    ]
    logger.info(f'启动 Locust workers 命令行: {str(args)}')
    worker_ps = []
    index = 1
    for _ in range(workers):
        env['LOCUST_INDEX'] = str(index) 
        worker_ps.append(subprocess.Popen(
            args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            #stderr=subprocess.DEVNULL,
            env=env 
        ))
        index += 1

    # Run locust master
    logger.info('启动 Locust master ...')
    args = [
        'locust',
        '--master',
        '--master-bind-port', f'{cfg.service_config.locust.master_port}',
        '--expect-workers', f'{workers}',
        '--headless',
        '-f', locustfile,
        '-H', url,
        '--csv', output_dir / 'locust',
        '--csv-full-history',
    ]
    master_p = subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL,
        env=env
    )

    logger.info(f'等待进程启动 ({output_dir})')
    while True:
        if os.path.exists(output_dir):
            break
        time.sleep(10) 
    
    logger.info('Locust 与 otelcol 启动成功')
    time.sleep(1)
    return master_p, worker_ps, otelcol_p


class ModificationHandler(FileSystemEventHandler):
    def __init__(self, file, callback, logger: Logger):
        self.__logger = logger 
        self.__file = file 
        self.__callback = callback 
        
        self.__logger.info(f'启动 watchdog 监控文件 {self.__file} 变化')
        self.__observer = Observer()
        self.__observer.schedule(self, self.__file, recursive=False)
        try:
            self.__observer.start()
        except FileNotFoundError:
            self.__logger.info(f'watchdog 未找到文件{self.__file}') 
    
    def on_modified(self, event):
        # TODO 只有修改后文件长度大于0才会出发回调函数
        if os.path.getsize(self.__file) == 0:
            return 
        self.__callback(event) 


class Locust(FileSystemEventHandler):
    def __init__(self, cfg, output_dir, dataset, logger: Logger):
        self.__output_dir = output_dir
        self.__dataset = dataset 
        self.__cfg = cfg 
        self.__logger = logger 
        
        self.__master_p = None 
        self.__worker_ps = None 
        self.__otelcol_p = None 
        
        self.__file_oberservers = list()
        self.__started = False
        
        self.__stats_file = pathlib.Path(self.__cfg.locust.output_dir) / self.__output_dir / 'locust_stats.csv' 
        self.__requests_file = pathlib.Path(self.__cfg.locust.output_dir) / self.__output_dir / 'requests.csv' 
        
    def start(self):
        self.__master_p, self.__worker_ps, self.__otelcol_p = with_locust(
                pathlib.Path(self.__cfg.locust.output_dir) / self.__output_dir,  
                f'{self.__dataset}.txt', 
                self.__cfg,
                self.__logger)
        self.__started = True 
        
    def wait(self): 
        while True:
            if self.__master_p.poll() is not None:
                break 
            time.sleep(1) 
        
    def stop(self):
        self.__master_p.kill()
        self.__logger.info('等待 locust master 退出')
        while True:
            if self.__master_p.poll() is not None:
                break 
            time.sleep(1)
        
        self.__logger.info('等待 locust workers 退出')
        for p in self.__worker_ps:
            p.kill()
            p.wait()
        
        self.__logger.info('等待 otelcol 退出')
        self.__otelcol_p.kill()
        self.__otelcol_p.wait() 
        
        self.__started = False 
        
    def read_stats_file(self) -> pd.DataFrame:
        return pd.read_csv(pathlib.Path(self.__stats_file)) 
    
    def read_requests(self) -> pd.DataFrame | None:
        request = None 
        try:
            request = pd.read_csv(pathlib.Path(self.__requests_file))
        except:
            self.__logger.error(f'read {self.__requests_file} failed')
        return request 
    
    def register_observer_on_requests(self, callback):
        if not self.__started:
            raise Exception('Locust 未启动')
        
        while True:
            if not os.path.exists(self.__requests_file):
                print(f'等待文件{self.__requests_file}被创建')
                time.sleep(1)
                continue
            break 
        
        handler = ModificationHandler(
            pathlib.Path(self.__requests_file),
            callback,
            self.__logger.getChild('Observer')
        )
        self.__file_oberservers.append(handler)
        
    def register_observer_on_stats_file(self, callback):
        if not self.__started:
            raise Exception('Locust 未启动')
        
        handler = ModificationHandler(
            pathlib.Path(self.__stats_file),
            callback,
            self.__logger.getChild('Observer')
        )
        self.__file_oberservers.append(handler)


