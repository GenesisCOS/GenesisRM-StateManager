from django.urls import path

from . import views


urlpatterns = [
    path('', views.index, name='index'),
    path('deployment/replicas/<str:namespace>/<str:name>', views.deployment_replicas, name='deployment-replicas'),
    path('deployment/replicas/operate', views.deployment_replicas_operate, name='deployment-replicas-ops'),
    path('deployment/names', views.deployment_names, name='deployment-names'),
    path('service/<str:name>/endpoints', views.service_endpoints, name='service-endpoints')
]
