from django.db import models


class DeploymentReplicasOperation(models.Model):
    name = models.CharField(max_length=100)
    namespace = models.CharField(max_length=100)
    target_replicas = models.IntegerField()
    target = models.CharField(max_length=100)


class Deployment(models.Model):
    name = models.CharField(max_length=100)
    namespace = models.CharField(max_length=100)
