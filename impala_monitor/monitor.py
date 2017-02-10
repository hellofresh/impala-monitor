import json
import concurrent.futures
import requests


class ImpalaMonitor(object):
    def __init__(self, nodes, graphite_node):
        self._nodes = nodes
        self._graphite_node = graphite_node

    def run(self):
        nodes = self.parse_nodes(self._nodes)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.load_url, node, 30): node for node in nodes}

            for future in concurrent.futures.as_completed(futures):
                node = futures[future]

                try:
                    data = future.result()

                    if data:
                        print("{} SEND TO STATSD".format(node))

                except Exception as exc:
                    print("Something went wrong {}".format(exc))

    @staticmethod
    def load_url(node, timeout):
        url = "{schema}{ip}/{path}".format(
            schema='http://',
            ip=node,
            path='jsonmetrics'
        )

        request = requests.get(url)
        if request.status_code == 200:
            return json.loads(request.text)

        return False

    def parse_nodes(self, nodes):
        return nodes.split(",")

