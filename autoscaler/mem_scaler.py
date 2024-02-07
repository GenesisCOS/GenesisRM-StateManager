import os 
import copy 
import json 
import time 
import subprocess
import pathlib 
from typing import List 
from logging import Logger 

import numpy as np 

from . import Scaler 


SCALER_DATA = pathlib.Path('autoscaler/mem_scaler_data')


def with_locust(data_dir, locustfile, url):
    
    env = copy.deepcopy(os.environ)
    env['TARGET_HOST'] = url 
    env['DATA_DIR'] = 'autoscaler/mem_scaler_data/' + data_dir 
    env['WAIT_TIME'] = f'{0}'
    
    args = [
        'locust',
        '--headless',
        '-f', locustfile,
        '--csv', SCALER_DATA/(data_dir + '/locust'),
        '--csv-full-history',
    ]
    master_p = subprocess.Popen(
        args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL,
        env=env
    )

    time.sleep(1)
    return master_p


class MemoryStat(object):
    def __init__(self, file) -> None:
        self.__file = file 
        self.__pgpgin = 0
        self.__pgpgout = 0
        self.__swap = 0
        self.__rss = 0
        self.__cache = 0
        self.__pgfault = 0
        
    def load(self):
        with open(self.__file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if line.startswith('swap '):
                    self.__swap = int(line[len("swap") + 1:-1])
                if line.startswith('rss '):
                    self.__rss = int(line[len("rss") + 1:-1])
                if line.startswith('cache '):
                    self.__cache = int(line[len("cache") + 1:-1])
                if line.startswith('pgpgin '):
                    self.__pgpgin = int(line[len("pgpgin") + 1:-1])
                if line.startswith('pgpgout '):
                    self.__pgpgout = int(line[len("pgpgout") + 1:-1])
                if line.startswith('pgfault '):
                    self.__pgfault = int(line[len("pgfault") + 1:-1])
    
    @property
    def pgpgin(self):
        return self.__pgpgin 
    
    @property
    def pgpgout(self):
        return self.__pgpgout 
    
    @property
    def swap(self):
        return self.__swap 
    
    @property
    def pgfault(self):
        return self.__pgfault


def read_memory_stat(file) -> MemoryStat:
    mem_stat = MemoryStat(file)
    mem_stat.load() 
    return mem_stat


class Service(object):
    def __init__(self, name, pod) -> None:
        self.__name = name 
        self.__pod = pod 
        
    @property
    def pod_uid(self):
        return self.__pod.metadata.uid.replace('-', '_')
    
    @property
    def pod_qos(self):
        return self.__pod.status.qos_class.lower()
    
    @property
    def container_name(self):
        return self.__name + '-service'
    
    @property
    def deploy_name(self):
        return self.__name + '-dep'
    
    @property
    def container_id(self):
        for container in self.__pod.status.container_statuses:
            if container.name == self.container_name:
                return container.container_id.split('//')[1]
            
    def set_memory_high(self, value):
        print(f'set service {self.__name} memory.high to {value}')
        mem_cgroup_path = f'/sys/fs/cgroup/memory/kubepods.slice/kubepods-{self.pod_qos}.slice/kubepods-{self.pod_qos}-pod{self.pod_uid}.slice/cri-containerd-{self.container_id}.scope'
        memory_high = mem_cgroup_path + '/memory.high'
            
        with open(memory_high, 'w') as file:
            file.write(f'{value}')
            file.flush()


class MemScaler(Scaler):
    def __init__(self, cfg, logger: Logger):
        super().__init__(cfg, logger) 
        
        self.__services = [
            'travel-plan', 'basic', 'ticketinfo', 'seat', 'station',
            'order', 'route-plan', 'route', 'config'
        ]
        
        self.__logger = logger
        self.__enabled_service_config = cfg.enabled_service_config
        self.__locust_url = cfg.base.locust.url 
        self.__locustfile = pathlib.Path(f'autoscaler/locust/{self.__enabled_service_config}')/'locustfile.py'
        
    def pre_start(self):
        pass 
        
    def start(self):
        services: List[Service] = list()
        for service in self.__services:
            services.append(Service(
                service,
                self.list_pods_of_dep(f'{service}-dep', 'train-ticket', None).items[0]))
        
        mem_high_value = 0 * 1024 * 1024
        with_swap_rt_p99 = list()
        with_swap_rt_avg = list()
        without_swap_rt_p99 = list()
        without_swap_rt_avg = list()
        for _ in range(20):
            for with_swap in [True, False]:
                __start_time = time.time()
                self.__logger.info(f'with swap = {with_swap}')
                os.mkdir(SCALER_DATA/'runtime'/f'data-{__start_time}')
                
                self.__logger.info(f'set memory.high to {mem_high_value}')
                for service in services:
                    service.set_memory_high(mem_high_value)
                
                wait_seconds = 10
                self.__logger.info(f'wait for {wait_seconds} seconds.')
                time.sleep(wait_seconds)
                
                """
                nr_in_out = 0
                nr_swap = 0
                nr_pgfault = 0
                prev_pgpgin, prev_pgpgout = None, None 
                prev_swap = None 
                prev_pgfault = None 
                for _ in range(1):
                    mem_stat = read_memory_stat(memory_stat)
                    pgpgin = mem_stat.pgpgin 
                    pgpgout = mem_stat.pgpgout
                    pgfault = mem_stat.pgfault
                    swap = mem_stat.swap  
                    
                    if prev_pgpgin is not None:
                        nr_in_out += abs(pgpgin - prev_pgpgin)
                    prev_pgpgin = pgpgin  
                    if prev_pgpgout is not None:
                        nr_in_out += abs(pgpgout - prev_pgpgout)
                    prev_pgpgout = pgpgout 
                    if prev_swap is not None:
                        nr_swap += abs(swap - prev_swap)
                    prev_swap = swap 
                    if prev_pgfault is not None:
                        nr_pgfault += abs(pgfault - prev_pgfault)
                    prev_pgfault = pgfault 
                    time.sleep(1)
                """
                
                self.__logger.info(f'set memory.high to max')
                for service in services:
                    service.set_memory_high('max')
                
                if with_swap:
                    self.__logger.info('turn swap off and on.')
                    os.system('swapoff -a')
                    os.system('swapon /home/swapfile')
                    
                p = with_locust(f'runtime/data-{__start_time}', self.__locustfile, self.__locust_url) 
                
                with p:
                    while True:
                        if p.poll() is not None:
                            break 
                        time.sleep(1)
                
                rt = list()
                with open(SCALER_DATA/f'runtime/data-{__start_time}/request.log', 'r') as file:
                    for line in file.readlines():
                        record = json.loads(line)
                        if record['name'] == '/api/v1/travelplanservice/travelPlan/cheapest':
                            rt.append(record['latency'])
                rt = np.array(rt)
                avg = np.mean(rt)
                p99 = np.percentile(rt, 99)
                
                if with_swap:
                    with_swap_rt_avg.append(avg)
                    with_swap_rt_p99.append(p99)
                    print(f'with swap rt p99 = {p99}')
                    print(f'with swap rt avg = {avg}')
                else:
                    without_swap_rt_avg.append(avg)
                    without_swap_rt_p99.append(p99)
                    print(f'without swap rt p99 = {p99}')
                    print(f'without swap rt avg = {avg}')
                
        print('-------- RESULT ---------')
        print(f'with swap rt p99    = {np.mean(with_swap_rt_p99)}')
        print(f'with swap rt avg    = {np.mean(with_swap_rt_avg)}')
        print(f'without swap rt p99 = {np.mean(without_swap_rt_p99)}')
        print(f'without swap rt avg = {np.mean(without_swap_rt_avg)}')
                
                    
