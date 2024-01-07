import argparse


class LabelDataPoint(object):
    def __init__(self,
                 response_time: float,
                 process_time: float,
                 throughput: float,
                 queueing: bool):
        
        self.response_time = response_time
        self.process_time = process_time
        self.throughput = throughput
        self.queueing = queueing


class PerfQueueing(object):
    def __init__(self):
        pass


def parse_args():
    parser = argparse.ArgumentParser('Perf Queueing')
    parser.add_argument('--service-name', type=str, dest='service_name', required=True, help='Service name')
    return parser.parse_args()


def main():
    args = parse_args()
     


