import sys 
import os 
import logging 
import requests 
import json 
import time 

import tqdm 
import numpy as np 
import pandas as pd 
import hydra
from omegaconf import DictConfig


@hydra.main(version_base=None, config_path='.', config_name='config')
def main(cfg: DictConfig):
    logger = logging.getLogger(name='PrometheusDownloader') 
    logger.info(f'Start time: {cfg.start_time}')
    logger.info(f'End time:   {cfg.end_time}')
    logger.info(f'host:       {cfg.host}')
    
    g_data = dict()
    
    _start = cfg.start_time 
    _end = min(_start + 1000, cfg.end_time)
    
    jobs = [
        dict(name='cpu_usage', query='rate(swiftmonitor_cpu_usage{namespace=\"train-ticket\"}[10s])/1000000000'),
        dict(name='memory_stat_usage', query='swiftmonitor_memory_stat_usage_in_bytes{namespace=\"train-ticket\"}'),
        dict(name='memory_stat_swap_usage', query='swiftmonitor_memory_stat_swap_in_bytes{namespace=\"train-ticket\"}'),
        dict(name='memory_stat_cache_usage', query='swiftmonitor_memory_stat_cache_in_bytes{namespace=\"train-ticket\"}'),
        dict(name='memory_stat_rss_usage', query='swiftmonitor_memory_stat_rss_in_bytes{namespace=\"train-ticket\"}'),
        dict(name='pod_cpu_limit', query='swiftmonitor_pod_cpu_limit{namespace=\"train-ticket\"}'),
        dict(name='cpu_limit', query='swiftmonitor_cpu_limit{namespace=\"train-ticket\"}'),
        dict(name='memory_limit', query='swiftmonitor_memory_limit_in_bytes{namespace=\"train-ticket\"}'),
        dict(name='cpu_allocated', query='swiftmonitor_k8s_pod_cpu_allocated{namespace=\"train-ticket\"}'),
        dict(name='memory_allocated', query='swiftmonitor_k8s_pod_memory_allocated{namespace=\"train-ticket\"}')
    ]

    for job in jobs:
        data = dict()
        print(f'job {job}')
        pbar = tqdm.tqdm(total=cfg.end_time - cfg.start_time + 1)
        
        while _start < cfg.end_time:
            resp = requests.get(
                url=cfg.host + '/api/v1/query_range',
                params={
                    'query': job.get('query'),
                    'start': f'{_start}',
                    'end': f'{_end}',
                    'step': '1s'
                }
            )
            pbar.update(_end - _start + 1)
            
            resp = json.loads(resp.text)
            if resp.get('status') == 'success':
                result_type = resp.get('data').get('resultType')
                results = resp.get('data').get('result')
                
                if result_type == 'matrix':

                    for result in results:
                        _values = list()
                        timestamps = list()
                        
                        metric = result.get('metric')
                        values = result.get('values')
                        
                        for el in values:
                            timestamps.append(el[0])
                            _values.append(float(el[1]))
                        
                        key = metric.get('podname') + '/' + metric.get('namespace')
                        
                        if key not in data:
                            data[key] = dict(
                                value=np.array(_values),
                                timestamps=np.array(timestamps)
                            )
                        else:
                            data.get(key)['value'] = \
                                np.append(data.get(key)['value'], np.array(_values))
                            data.get(key)['timestamps'] = \
                                np.append(data.get(key)['timestamps'], np.array(timestamps))
                else:
                    raise Exception(f'Result type is not matrix but {result_type}')
            else:
                print(resp)
                raise Exception(f'Prometheus return status={resp.get("status")} job={job}')
            
            _start = _end + 1
            _end = min(_start + 1000, cfg.end_time)
        g_data[job.get('name')] = data 
        
        _start = cfg.start_time 
        _end = min(_start + 1000, cfg.end_time)
    
    root_directory = f'data-{int(time.time())}'
    os.mkdir(root_directory)
    for name, data in g_data.items():
        os.mkdir(f'{root_directory}/{name}')
        for key, _data in data.items():
            df = pd.DataFrame(_data)
            df.set_index('timestamps', inplace=True)
            csv_file_name = key.replace('/', '@')
            df.to_csv(f'{root_directory}/{name}/{csv_file_name}')



if __name__ == '__main__':
    sys.exit(main())


