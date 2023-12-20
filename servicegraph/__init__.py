from typing import List

from py2neo import Graph, Node, NodeMatcher, RelationshipMatcher


class ServiceGraph(object):
    def __init__(self, neo4j_url):
        self.driver = Graph(neo4j_url)
        self.node_matcher = NodeMatcher(self.driver)
        self.relationship_matcher = RelationshipMatcher(self.driver)

    def match_services(self, project_name: str, service_name: str = None) -> List[Node] | None:
        properties = dict()
        if project_name is None:
            return None
        properties["app_name"] = project_name
        if service_name is not None:
            properties["name"] = service_name
        return [s for s in self.node_matcher.match("Service", **properties)]

    def match_endpoints(self, service: Node) -> List:
        relas = self.relationship_matcher.match((service,), r_type="HAS")
        return [i.end_node for i in relas]

    def match_service_by_endpoint(self, endpoint: Node):
        relas = self.relationship_matcher.match((None, endpoint), r_type="HAS")
        return [i.start_node for i in relas]

    def match_downstream_endpoints(self, endpoint):
        relas = self.driver.match((endpoint,), "INVOKE")
        return [i.end_node for i in relas]
