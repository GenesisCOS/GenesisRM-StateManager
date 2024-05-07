import sys
import os
import re
import threading
import multiprocessing
import pandas as pd
import hydra
from omegaconf import DictConfig


hpa_fix = {
    'assurance': 0.5,
    'auth': 0.5,
    'basic': 2.2,
    'config': 0.71,
    'food-map': 0.51,
    'food': 0.71,
    'order-other': 0.5,
    'order': 0.85,
    'preserve-other': 0.5,
    'preserve': 0.5,
    'price': 0.2,
    'route-plan': 0.65,
    'route': 1.9,
    'seat': 1.9,
    'station': 2.1,
    'ticketinfo': 2.1,
    'train': 0.95,
    'travel-b': 2.1,
    'travel-plan': 0.98,
    'travel': 2.0,
    'travel2-b': 0.5,
    'travel2': 0.83,
    'user': 0.5,
    'verify-code': 0.5
}


def process_metric(data_dir, output_dir, exp, metric_name, hpa_fix):
    print('Loading metric ', metric_name)
    metrics = dict()
    # load csv
    idx = 0
    for file in os.listdir(os.path.join(data_dir, metric_name)):
        file_path = os.path.join(data_dir, metric_name, file)

        if os.path.isfile(file_path):
            match_obj = re.findall(r'([a-zA-Z0-9-]*)-dep-[a-zA-Z0-9]*-[a-zA-Z0-9]*@[a-zA-Z-]*', file)
            if len(match_obj) == 0:
                continue
            service = match_obj[0]
            if service not in metrics:
                metrics[service] = list()
            print(f'Loading csv {file_path}')
            _data = pd.read_csv(file_path)
            _data.rename(columns={'value': f'value-{idx}'}, inplace=True)
            _data.set_index('timestamps', inplace=True)

            print('Fixing values.')
            if (exp == 'ahpa' or exp == 'swiftkube') and metric_name == 'memory_allocated':
                for i in range(len(_data[f'value-{idx}'].values)):
                    _data[f'value-{idx}'].values[i] = min(3221225472.0, _data[f'value-{idx}'].values[i])

            if (exp == 'ahpa' or exp == 'swiftkube') and metric_name == 'memory_limit':
                for i in range(len(_data[f'value-{idx}'].values)):
                    _data[f'value-{idx}'].values[i] = min(3221225472.0, _data[f'value-{idx}'].values[i])

            if exp == 'hpa' and metric_name == 'cpu_limit':
                fix_value = hpa_fix[service]
                for i in range(len(_data[f'value-{idx}'].values)):
                    _data[f'value-{idx}'].values[i] = fix_value

            metrics[service].append(_data)
        idx += 1

    # aggregate
    os.mkdir(os.path.join(output_dir, metric_name))
    _data_all = None
    for service, df_list in metrics.items():
        _data = None
        for df in df_list:
            if _data is None:
                _data = df
            else:
                _data = pd.merge(_data, df, how='outer', on='timestamps').fillna(0)

            if _data_all is None:
                _data_all = df
            else:
                _data_all = pd.merge(_data_all, df, how='outer', on='timestamps').fillna(0)
        _data.sum(axis=1).to_csv(os.path.join(output_dir, metric_name, service + '.csv'))
        print('Writing csv ', os.path.join(output_dir, metric_name, service + '.csv'))
    if _data_all is not None:
        _data_all.sum(axis=1).to_csv(os.path.join(output_dir, metric_name, 'all.csv'))
        print('Writing csv ', os.path.join(output_dir, metric_name, 'all.csv'))


@hydra.main(version_base=None, config_path='.', config_name='config')
def main(cfg: DictConfig):
    data_dir = cfg.aggregate.dir
    output_dir = cfg.aggregate.out
    exp = cfg.aggregate.exp

    os.mkdir(output_dir)

    processes = []
    for metric_dir in os.listdir(data_dir):
        if os.path.isdir(os.path.join(data_dir, metric_dir)):
            p = multiprocessing.Process(target=process_metric, args=(data_dir, output_dir, exp, metric_dir, hpa_fix))
            processes.append(p)
            p.start()

    for p in processes:
        p.join()


if __name__ == '__main__':
    sys.exit(main())

