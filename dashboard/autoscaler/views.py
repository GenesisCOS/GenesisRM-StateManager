from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.conf import settings
from rest_framework.decorators import api_view
from rest_framework.parsers import JSONParser
from rest_framework import status
import grpc

from .serializers import DeploymentReplicasOperationSerializer
from proto import elastic_deployment_pb2 as ed_pb
from proto import elastic_deployment_pb2_grpc as ed_grpc
from servicegraph import ServiceGraph


SERIALIZE_ERROR_CODE = 1


@api_view(['GET'])
def index(request):
    return HttpResponse('MicroKube Autoscaler')


@api_view(['POST', 'OPTIONS'])
def deployment_replicas_operate(request):
    if request.method == 'OPTIONS':
        obj = HttpResponse()
        obj['Access-Control-Allow-Origin'] = '*'
        obj['Access-Control-Allow-Headers'] = 'Content-Type'
        obj['Access-Control-Max-Age'] = '2592000'
        obj['Access-Control-Allow-Methods'] = 'POST'
        return obj
    operation_data = JSONParser().parse(request)
    s = DeploymentReplicasOperationSerializer(data=operation_data)
    if s.is_valid():
        req = ed_pb.DeploymentReplicasOpsRequest(
            target=ed_pb.Deployment(name=s.data.get('name'), namespace=s.data.get('namespace')),
            target_replicas=s.data.get('target_replicas')
        )
        if s.data.get('target') == 'all':
            with grpc.insecure_channel(settings.MICROKUBE['elastic-deployment-url']) as channel:
                stub = ed_grpc.DeploymentOperationsStub(channel)
                resp = stub.SetPodReplicas(req)
            obj = JsonResponse(dict(err_code=resp.base.err_code, err_msg=resp.base.err_msg))
            obj['Access-Control-Allow-Origin'] = '*'
            return obj
        else:  # target == 'running'
            with grpc.insecure_channel(settings.MICROKUBE['elastic-deployment-url']) as channel:
                stub = ed_grpc.DeploymentOperationsStub(channel)
                resp = stub.SetRunningPodReplicas(req)
            obj = JsonResponse(dict(err_code=resp.base.err_code, err_msg=resp.base.err_msg))
            obj['Access-Control-Allow-Origin'] = '*'
            return obj
    else:
        obj = JsonResponse(
            dict(
                err_code=SERIALIZE_ERROR_CODE,
                err_msg='serialize error',
                errors=s.errors
            ), status=status.HTTP_400_BAD_REQUEST
        )
        obj['Access-Control-Allow-Origin'] = '*'
        return obj


@api_view(['GET'])
def deployment_replicas(request, name, namespace):
    with grpc.insecure_channel(settings.MICROKUBE['elastic-deployment-url']) as channel:
        stub = ed_grpc.DeploymentOperationsStub(channel)
        resp = stub.GetPodReplicas(ed_pb.Deployment(
            name=name,
            namespace=namespace
        ))
        all_replicas = resp.replicas
        resp = stub.GetRunningPodReplicas(ed_pb.Deployment(
            name=name,
            namespace=namespace
        ))
        running = resp.replicas
        resp = stub.GetRunnablePodReplicas(ed_pb.Deployment(
            name=name,
            namespace=namespace
        ))
        runnable = resp.replicas
        resp = stub.GetInitializingPodReplicas(ed_pb.Deployment(
            name=name,
            namespace=namespace
        ))
        initializing = resp.replicas

    obj = JsonResponse(dict(
        err_code=0, err_msg='success',
        data=dict(
            all=all_replicas,
            running=running,
            runnable=runnable,
            initializing=initializing
        )
    ))
    obj['Access-Control-Allow-Origin'] = '*'
    return obj


@api_view(['GET'])
def deployment_names(request):
    with grpc.insecure_channel(settings.MICROKUBE['elastic-deployment-url']) as channel:
        stub = ed_grpc.DeploymentOperationsStub(channel)
        resp = stub.GetNames(ed_pb.EmptyRequest())
    obj = JsonResponse(dict(
        names=[name for name in resp.names]
    ))
    obj['Access-Control-Allow-Origin'] = '*'
    return obj


@api_view(['GET'])
def service_endpoints(request, name):
    sg = ServiceGraph(neo4j_url=settings.MICROKUBE['neo4j-url'])
    service_node = sg.match_services('trainticket', name)
    if len(service_node) == 0:
        obj = JsonResponse(dict(
            endpoints=list()
        ))
        obj['Access-Control-Allow-Origin'] = '*'
        return obj
    endpoints = sg.match_endpoints(service_node[0])
    obj = JsonResponse(dict(
        endpoints=[dict(uri=endpoint['uri']) for endpoint in endpoints]
    ))
    obj['Access-Control-Allow-Origin'] = '*'
    return obj
