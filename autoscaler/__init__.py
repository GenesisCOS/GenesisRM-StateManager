'''
import grpc

from proto import elastic_deployment_pb2 as ed_pb
from proto import elastic_deployment_pb2_grpc as ed_grpc
from proto import microkube_autoscaler_pb2 as ma_pb
from proto import microkube_autoscaler_pb2_grpc as ma_grpc
'''


class LongTermTimeSeriesEstimator(object):
    pass


class ShortTermTimeSeriesEstimator(object):
    pass

'''
class DeploymentOperations(object):
    def __init__(self, target):
        self.target = target

    def set_pod_replicas(self, name: str, namespace: str, target_replicas: int) -> ed_pb.DeploymentOpsResponse:
        with grpc.insecure_channel(self.target) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.SetPodReplicas(ed_pb.DeploymentReplicasOpsRequest(
                target=ed_pb.Deployment(name=name, namespace=namespace),
                target_replicas=target_replicas
            ))
        return resp

    def set_running_pod_replicas(self, name: str, namespace: str, target_replicas: int) -> ed_pb.DeploymentOpsResponse:
        with grpc.insecure_channel(self.target) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.SetRunningPodReplicas(ed_pb.DeploymentReplicasOpsRequest(
                target=ed_pb.Deployment(name=name, namespace=namespace),
                target_replicas=target_replicas
            ))
        return resp

    def get_running_pod_replicas(self, name: str, namespace: str) -> ed_pb.DeploymentGetReplicasResponse:
        with grpc.insecure_channel(self.target) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.GetRunningPodReplicas(ed_pb.Deployment(
                name=name,
                namespace=namespace
            ))
        return resp

    def get_runnable_pod_replicas(self, name: str, namespace: str) -> ed_pb.DeploymentGetReplicasResponse:
        with grpc.insecure_channel(self.target) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.GetRunnablePodReplicas(ed_pb.Deployment(
                name=name,
                namespace=namespace
            ))
        return resp

    def get_initializing_pod_replicas(self, name: str, namespace: str) -> ed_pb.DeploymentGetReplicasResponse:
        with grpc.insecure_channel(self.target) as channel:
            stub = ed_grpc.DeploymentOperationsStub(channel)
            resp = stub.GetInitializingPodReplicas(ed_pb.Deployment(
                name=name,
                namespace=namespace
            ))
        return resp


class Autoscaler(object):
    def __init__(self, ed_url):
        r"""
        Autoscaler
        :param ed_url: elastic deployment url
        """
        self.ed_url = ed_url
        self.deployment_ops: DeploymentOperations = DeploymentOperations(target=self.ed_url)


class AutoscalerGrpcServicer(ma_grpc.AutoscalerServicer):
    def __init__(self, autoscaler: Autoscaler):
        self.autoscaler = autoscaler

    def IsReady(self, request, context):
        return ma_pb.BoolResponse(value=True)
'''
