from kubernetes import client, config, dynamic

configuration = client.Configuration()
configuration.verify_ssl = False 
configuration.cert_file = '/etc/kubernetes/ssl/cert/admin.pem'
configuration.key_file = '/etc/kubernetes/ssl/key/admin-key.pem'
configuration.host = 'https://192.168.137.138:6443'
client.Configuration.set_default(configuration)

import urllib3 
urllib3.disable_warnings()

# Configs can be set in Configuration class directly or using helper utility
# config.load_kube_config(config_file='/root/.kube/config')

# config.load_kube_config(client_configuration=configuration)

dynamic_kube_client = dynamic.DynamicClient(client.ApiClient())
swiftdeployment_api = dynamic_kube_client.resources.get(api_version='swiftkube.io/v1alpha1', kind='SwiftDeployment')
swiftdeployments = swiftdeployment_api.get()
for swiftdeployment in swiftdeployments.items:
    swiftdeployment.spec.replicas = 1
    swiftdeployment_api.patch(body=swiftdeployment, content_type='application/merge-patch+json')