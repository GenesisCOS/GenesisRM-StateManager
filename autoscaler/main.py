import sys
import os
import argparse
import json
import threading
import logging 
from concurrent import futures

# import grpc

# from . import Autoscaler
# from . import AutoscalerGrpcServicer
# from proto import microkube_autoscaler_pb2_grpc as ma_grpc
from .data import ServiceEndpointPair
from .ts_predictor.estimator import Estimator as SttfEstimator


parser = argparse.ArgumentParser('')
parser.add_argument(
    '--autoscaler', required=True, dest='autoscaler',
    default='MicroKube',
    help='Autoscaler, one of [MicroKube, AIMD-H, AIMD-V]'
)
parser.add_argument(
    '--informer-model', required=False, dest='informer_model',
    default='',
    help='Path of trained Informer model.'
)
parser.add_argument(
    '--lstm-model', required=False, dest='lstm_model',
    default='',
    help='Path of trained LSTM model.'
)
parser.add_argument(
    '--deepar-model', required=False, dest='deepar_model',
    default='',
    help='Path of trained DeepAR model.'
)
parser.add_argument(
    '--grpc-listen-port',
    type=int,
    required=False,
    dest='grpc_listen_port',
    default=10000,
    help='grpc server listen port (default: 10000)'
)
parser.add_argument(
    '--use-lttf',
    dest='use_lttf',
    action='store_true', default=False,
    help='use long term time-series forecasting model (default: True)'
)
parser.add_argument(
    '--lttf-model',
    dest='lttf_model',
    required=False,
    type=str, default='lstm',
    help='lttf model, one of [lstm, informer, deepar]'
)
parser.add_argument(
    '--use-sttf',
    dest='use_sttf',
    action='store_true', default=False,
    help='use short term time-series forecasting model (default: True)'
)
parser.add_argument(
    '--sttf-regressor',
    dest='sttf_regressor', default='svr',
    help='short term time-series forecasting model regressor, default is SVR'
)
parser.add_argument(
    '--elastic-deployment-url',
    dest='ed_url', required=False,
    help='GRPC url of elastic deployment module.'
)
parser.add_argument(
    '--postgresql-host',
    dest='postgres_host', required=True,
    help='PostgreSQL host'
)
parser.add_argument(
    '--postgresql-user',
    dest='postgres_user', required=True,
    help='PostgreSQL username'
)
parser.add_argument(
    '--postgresql-password',
    dest='postgres_passwd', required=True,
    help='PostgreSQL user password'
)
parser.add_argument(
    '--postgresql-port',
    dest='postgres_port', required=True,
    help='PostgreSQL port'
)
parser.add_argument(
    '--postgresql-database',
    dest='postgres_db', required=True,
    help='PostgreSQL database'
)
parser.add_argument(
    '--sttf-history-len',
    type=int,
    dest='sttf_history_len', required=False,
    help='sttf history data length'
)
parser.add_argument(
    '--sttf-alpha',
    type=float,
    dest='sttf_alpha', required=False,
    help='sttf history data length'
)
parser.add_argument(
    '--sttf-sample-x-len',
    type=int,
    dest='sttf_sample_x_len', required=False,
    help='sttf sample data length'
)
parser.add_argument(
    '--sttf-sample-y-len',
    type=int,
    dest='sttf_sample_y_len', required=False,
    help='sttf label data length'
)
parser.add_argument(
    '--l1-sync-period-sec',
    type=int,
    dest='l1_sync_period_sec', required=False,
    help='l1 scaling strategy sync period'
)
parser.add_argument(
    '--l2-sync-period-sec',
    type=int,
    dest='l2_sync_period_sec', required=False,
    help='l2 scaling strategy sync period'
)
parser.add_argument(
    '--elastic-deployment-host',
    type=str, dest='ed_host', required=False,
    help='elastic deployment host '
)
parser.add_argument(
    '--root-endpoint-list-file',
    dest='root_endpoints_file', required=True,
    help='File which list root endpoints'
)
parser.add_argument(
    '--service-config-file',
    dest='service_config_file', required=True,
    help='Service configurations such as max thread'
)
parser.add_argument(
    '--k8s-namespace',
    dest='k8s_namespace', required=True, type=str,
    help='kubernetes namespace'
)
parser.add_argument(
    '--neo4j-url', type=str, required=True,
    dest='neo4j_url',
    help='Neo4j URL'
)
parser.add_argument(
    '--neo4j-project-name', type=str, required=True,
    dest='neo4j_project_name',
    help='Neo4j project name'
)
parser.add_argument(
    '--aimd-period', type=int, required=False,
    dest='aimd_period',
    help='AIMD period'
)
parser.add_argument(
    '--log-level', type=str, required=False,
    default='info', 
    dest='log_level'  
)

args = parser.parse_args()

if args.log_level == 'info':
    level = logging.INFO 
elif args.log_level == 'debug':
    level = logging.DEBUG
else:
    raise Exception(f'Unknown log level: {args.log_level}')
    
logging.basicConfig(level=level,
                    format='%(asctime)s - %(filename)s - %(name)s - %(levelname)s> %(message)s')


project_path = os.path.split(os.path.realpath(__file__))[0] + '/../'
if project_path not in sys.path:
    sys.path.append(project_path)


def main():
    with open(args.root_endpoints_file, 'r') as f:
        content = f.read()
        root_endpoints_ = json.loads(content).get('root-endpoints')
    root_endpoints = list()
    for endpoint in root_endpoints_:
        root_endpoints.append(ServiceEndpointPair(
            svc_name=endpoint.get('service-name'),
            ep_name=endpoint.get('endpoint-name')
        ))

    with open(args.service_config_file, 'r') as f:
        content = f.read()
        service_configs = json.loads(content)

    sttf_estimator = None
    lttf_estimator = None

    if args.use_lttf:
        if args.lttf_model == 'informer' and args.informer_model == '':
            raise Exception('The trained Informer model path must be specified')
        elif args.lttf_model == 'lstm' and args.lstm_model == '':
            raise Exception('The trained LSTM model path must be specified')
        elif args.lttf_model == 'deepar' and args.lstm_model == '':
            raise Exception('The trained DeepAR model path must be specified')

        if args.lttf_model == 'lstm':
            from .ts_predictor.lstm import Predictor as LSTMPredictor
            lttf_estimator = LSTMPredictor(input_size=1, hidden_size=100, num_layers=1, output_size=1)
            lttf_estimator.to(lttf_estimator.device)
        else:
            raise Exception(f'do not support {args.lttf_model}')
    if args.use_sttf:
        sttf_estimator = SttfEstimator(args, logging.getLogger(name='STTF'))

    if args.autoscaler == 'MicroKube':
        from .scaler import MicroKubeScaler
        scaler = MicroKubeScaler(args, logging.getLogger(name='MicroKubeScaler'), root_endpoints, service_configs, sttf_estimator, lttf_estimator)
    elif args.autoscaler == 'AIMD-H':
        from .scaler import AIMDHScaler
        scaler = AIMDHScaler(args, logging.getLogger(name='AIMD-H-Scaler'), service_configs)
    elif args.autoscaler == 'AIMD-V':
        from .scaler import AIMDVScaler
        scaler = AIMDVScaler(args, logging.getLogger(name='AIMD-V-Scaler'), service_configs)
    else:
        raise Exception(f'unsupported autoscaler {args.autoscaler}')
    t = threading.Thread(target=scaler.start, name='scaler-thread', daemon=True)
    t.start()
    t.join()

    '''
    autoscaler = Autoscaler(args.ed_url)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ma_grpc.add_AutoscalerServicer_to_server(AutoscalerGrpcServicer(autoscaler), server)
    server.add_insecure_port('[::]:' + str(args.grpc_listen_port))
    server.start()
    print("Server started, listening on " + str(args.grpc_listen_port))
    server.wait_for_termination()
    '''


if __name__ == '__main__':
    print('Starting ...')
    sys.exit(main())
