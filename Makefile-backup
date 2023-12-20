INFORMER_MODEL := ./model/informer


__all:

.PHONY: grpc, run
grpc:
	python3 -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/proto/elastic-deployment.proto
	python3 -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/proto/microkube-autoscaler.proto

run:
	python3 dashboard/manage.py runserver

run_microkube_autoscaler:
	python3 -m autoscaler.main  \
		--autoscaler=MicroKube  \
		--informer-model=${INFORMER_MODEL} \
		--use-sttf \
		--elastic-deployment-url=localhost:30000 \
		--neo4j-url=bolt://222.201.144.196:7687 \
		--neo4j-project-name=trainticket \
		--postgresql-host=222.201.144.196  \
		--postgresql-user=postgres  \
		--postgresql-database=trainticket  \
		--postgresql-password=Admin@123_.  \
		--postgresql-port=5433 \
		--sttf-history-len=40  \
		--sttf-alpha=0.1  \
		--sttf-sample-x-len=10  \
		--sttf-sample-y-len=1  \
		--l1-sync-period-sec=5 \
		--l2-sync-period-sec=300 \
		--k8s-namespace=default  \
		--log-level=info \
		--root-endpoint-list-file=config/media-microservices/root-endpoints.json \
		--service-config-file=config/media-microservices/service-config.json

run_trainticket_microkube_autoscaler:
	python3 -m autoscaler.main  \
		--autoscaler=MicroKube  \
		--informer-model=${INFORMER_MODEL} \
		--use-sttf \
		--elastic-deployment-url=localhost:30000 \
		--neo4j-url=bolt://222.201.144.196:7687 \
		--neo4j-project-name=trainticket \
		--postgresql-host=222.201.144.196  \
		--postgresql-user=postgres  \
		--postgresql-database=trainticket  \
		--postgresql-password=Admin@123_.  \
		--postgresql-port=5433 \
		--sttf-history-len=40  \
		--sttf-alpha=0.1  \
		--sttf-sample-x-len=10  \
		--sttf-sample-y-len=1  \
		--l1-sync-period-sec=5 \
		--l2-sync-period-sec=300 \
		--k8s-namespace=default  \
		--log-level=info \
		--root-endpoint-list-file=config/trainticket/root-endpoints.json \
		--service-config-file=config/trainticket/service-config.json

run_aimd_h_autoscaler:
	python3 -m autoscaler.main  \
		--autoscaler=AIMD-H  \
		--neo4j-url=bolt://222.201.144.196:7687 \
		--neo4j-project-name=trainticket  \
		--postgresql-host=222.201.144.196  \
		--postgresql-user=postgres  \
		--postgresql-database=trainticket  \
		--postgresql-password=Admin@123_.  \
		--postgresql-port=5433 \
		--k8s-namespace=default  \
		--aimd-period=10 \
		--root-endpoint-list-file=config/media-microservices/root-endpoints.json \
		--service-config-file=config/media-microservices/service-config.json

run_aimd_v_autoscaler:
	python3 -m autoscaler.main  \
		--autoscaler=AIMD-V  \
		--neo4j-url=bolt://222.201.144.196:7687 \
		--neo4j-project-name=trainticket  \
		--postgresql-host=222.201.144.196  \
		--postgresql-user=postgres  \
		--postgresql-database=trainticket  \
		--postgresql-password=Admin@123_.  \
		--postgresql-port=5433 \
		--k8s-namespace=default  \
		--aimd-period=10 \
		--root-endpoint-list-file=config/media-microservices/root-endpoints.json \
		--service-config-file=config/media-microservices/service-config.json