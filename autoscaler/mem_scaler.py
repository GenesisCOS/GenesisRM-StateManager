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
        #stderr=subprocess.DEVNULL,
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
    def name(self):
        return self.__name 
    
    @property
    def container_id(self):
        for container in self.__pod.status.container_statuses:
            if container.name == self.container_name:
                return container.container_id.split('//')[1]
            
    def get_memory_stat(self):
        mem_cgroup_path = f'/sys/fs/cgroup/memory/kubepods.slice/kubepods-{self.pod_qos}.slice/kubepods-{self.pod_qos}-pod{self.pod_uid}.slice/cri-containerd-{self.container_id}.scope'
        memory_stats = mem_cgroup_path + '/memory.stat'
        return read_memory_stat(memory_stats)
            
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
        
        with_swap_rt_p99 = list()
        with_swap_rt_avg = list()
        without_swap_rt_p99 = list()
        without_swap_rt_avg = list()
        with_swap = True
        for _ in range(20):
            for mem_high_value in [
                0 * 1024 * 1024,
                10 * 1024 * 1024,
                20 * 1024 * 1024,
                30 * 1024 * 1024,
                40 * 1024 * 1024,
                50 * 1024 * 1024,
                60 * 1024 * 1024,
                70 * 1024 * 1024,
                80 * 1024 * 1024,
                90 * 1024 * 1024,
                100 * 1024 * 1024,
                110 * 1024 * 1024,
                120 * 1024 * 1024,
                130 * 1024 * 1024,
                140 * 1024 * 1024,
                150 * 1024 * 1024,
                160 * 1024 * 1024,
                170 * 1024 * 1024,
                180 * 1024 * 1024,
                190 * 1024 * 1024,
                200 * 1024 * 1024,
                300 * 1024 * 1024,
                400 * 1024 * 1024,
                500 * 1024 * 1024,
                1024 * 1024 * 1024,
                2048 * 1024 * 1024
            ]:
                __start_time = time.time()
                self.__logger.info(f'with swap = {with_swap}')
                os.mkdir(SCALER_DATA/'runtime'/f'data-{__start_time}')
                
                self.__logger.info(f'set memory.high to max')
                for service in services:
                    service.set_memory_high('max')
                
                p = with_locust(f'runtime/data-{__start_time}', self.__locustfile, self.__locust_url) 
                with p:
                    while True:
                        if p.poll() is not None:
                            break 
                        time.sleep(1)
                
                self.__logger.info(f'set memory.high to {mem_high_value}')
                for service in services:
                    service.set_memory_high(mem_high_value)
                
                wait_seconds = 10
                self.__logger.info(f'wait for {wait_seconds} seconds.')
                time.sleep(wait_seconds)
                
                mem_stat_cache = dict()
                for service in services:
                    mem_stat_cache[service.name] = dict(
                        prev_pgpgin=None,
                        prev_pgpgout=None,
                        nr_in_out=0,
                        pgpgin_per_second=0,
                        pgpgout_per_second=0
                    )
                with open(f'result-{int(mem_high_value / 1024 / 1024)}.json', 'w+') as result_file:
                    for _ in range(60 * 30):
                        for service in services:
                            mem_stat = service.get_memory_stat()
                            pgpgin = mem_stat.pgpgin 
                            pgpgout = mem_stat.pgpgout 
                            
                            if mem_stat_cache[service.name]['prev_pgpgin'] is not None:
                                mem_stat_cache[service.name]['nr_in_out'] += \
                                    abs(pgpgin - mem_stat_cache[service.name]['prev_pgpgin'])
                                mem_stat_cache[service.name]['pgpgin_per_second'] = \
                                    abs(pgpgin - mem_stat_cache[service.name]['prev_pgpgin'])
                            mem_stat_cache[service.name]['prev_pgpgin'] = pgpgin  
                            if mem_stat_cache[service.name]['prev_pgpgout'] is not None:
                                mem_stat_cache[service.name]['nr_in_out'] += \
                                    abs(pgpgout - mem_stat_cache[service.name]['prev_pgpgout'])
                                mem_stat_cache[service.name]['pgpgout_per_second'] = \
                                    abs(pgpgout - mem_stat_cache[service.name]['prev_pgpgout'])
                            mem_stat_cache[service.name]['prev_pgpgout'] = pgpgout 
                        print(mem_stat_cache)
                        result = json.dumps(mem_stat_cache)
                        result_file.write(result + '\n')
                        result_file.flush()
                        time.sleep(1)
                
                
                self.__logger.info(f'set memory.high to max')
                for service in services:
                    service.set_memory_high('max')
                
                if with_swap:
                    self.__logger.info('turn swap off and on.')
                    os.system('swapoff -a')
                    os.system('swapon /home/swapfile')
                
                """ 
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
                """
        """
        print('-------- RESULT ---------')
        print(f'with swap rt p99    = {np.mean(with_swap_rt_p99)}')
        print(f'with swap rt avg    = {np.mean(with_swap_rt_avg)}')
        print(f'without swap rt p99 = {np.mean(without_swap_rt_p99)}')
        print(f'without swap rt avg = {np.mean(without_swap_rt_avg)}')
        """
                    
