from kubernetes import client, config

configuration = client.Configuration()
configuration.verify_ssl = False 
configuration.cert_file = '/etc/kubernetes/ssl/cert/admin.pem'
configuration.key_file = '/etc/kubernetes/ssl/key/admin-key.pem'
configuration.host = 'https://192.168.137.138:6443'
client.Configuration.set_default(configuration)

# Configs can be set in Configuration class directly or using helper utility
# config.load_kube_config(config_file='/root/.kube/config')

# config.load_kube_config(client_configuration=configuration)

v1 = client.CoreV1Api()
print("Listing pods with their IPs:")
ret = v1.list_pod_for_all_namespaces(watch=False)
for i in ret.items:
    print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))