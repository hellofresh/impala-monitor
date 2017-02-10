import json
import concurrent.futures
import requests
import statsd

from .stats import ImpalaStats


class ImpalaMonitor(object):
    def __init__(self, nodes, graphite_node, environment='staging'):
        self._nodes = self.parse_nodes(nodes)
        self._graphite_node = graphite_node
        self._statsd = statsd.StatsClient(
            graphite_node, 8125, 'dwh.{}.impala'.format(environment)
        )

        self._stats = ImpalaStats(self._statsd)

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.load_url, node, 30): node for 
                       node in self._nodes}

            for future in concurrent.futures.as_completed(futures):
                node = futures[future]

                try:
                    data = future.result()

                    if data:
                        self._stats.send(node, data)

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

