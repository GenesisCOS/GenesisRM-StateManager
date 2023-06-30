from rest_framework import serializers

from .models import DeploymentReplicasOperation, Deployment


class DeploymentReplicasOperationSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeploymentReplicasOperation
        fields = ('id', 'name', 'namespace', 'target_replicas', 'target')


class DeploymentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Deployment
        fields = ('id', 'name', 'namespace')
