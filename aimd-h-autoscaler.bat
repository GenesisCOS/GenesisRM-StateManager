python -m autoscaler.main  ^
		--autoscaler=AIMD-H  ^
		--neo4j-url=bolt://222.201.144.196:7687 ^
		--neo4j-project-name=trainticket  ^
		--postgresql-host=222.201.144.196  ^
		--postgresql-user=postgres  ^
		--postgresql-database=trainticket  ^
		--postgresql-password=Admin@123_.  ^
		--postgresql-port=5433 ^
		--elastic-deployment-url=localhost:30000 ^
		--elastic-deployment-host=222.201.144.237:60100 ^
		--k8s-namespace=default  ^
		--response-time-table=service_time_stats ^
		--task-stats-table=service_time_stats ^
		--task-stats-table2=response_time2  ^
		--request-num-table=response_time2  ^
		--request-num-table2=service_time_stats ^
		--aimd-period=10 ^
		--root-endpoint-list-file=config/media-microservices/root-endpoints.json ^
		--service-config-file=config/media-microservices/service-config.json

pause