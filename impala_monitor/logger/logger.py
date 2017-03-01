import concurrent.futures
import requests
from .parser import Query
from .parser import ImpalaQueryLogParser


class ImpalaLogger(object):
    def __init__(self, nodes: list, kibana_host: str, kibana_port: int):
        self.kibana_port = kibana_port
        self.kibana_host = kibana_host
        self.nodes = nodes

    def run(self):
        """
        We need to get from all the nodes the required information.
        In this case we need to first of all load the queries, and then the
        metadata on the query profile.

        :return:
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.query_retriever, node, 30): node for node
                in
                self.nodes
            }

            for future in concurrent.futures.as_completed(futures):
                node = futures[future]

                try:
                    print("I have all the data here!")
                except Exception as e:
                    print ("Something went wrong {}".format(e))

    @staticmethod
    def query_retriever(node: str, timeout: int) -> Query:
        url = "{schema}{ip}/{path}".format(
            schema='http://',
            ip=node,
            path='queries'
        )

        request = requests.get(url)
        if request.status_code != 200:
            return False

        parser = ImpalaQueryLogParser(request.text)
        queries = parser.queries

        if not queries:
            return False

        for query in queries:
            query = query.query
            query['node'] = node
            query = Query(query)

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
                futures = {exec.submit(ImpalaLogger.query_profiler, query): query in \
                                                                   queries}

                for future in concurrent.futures.as_completed(futures):
                    try:
                        query = future.result()
                        print(query.query_id)
                    except Exception as e:
                        print ("Something went wrong {}".format(e))

    @staticmethod
    def query_profiler(query: Query) -> Query:
        url = "{schema}{ip}/query_profile?query_id={query_id}".format(
            schema='http://',
            ip=query.node,
            query_id=query.query_id
        )

        request = requests.get(url)
        if request.status_code != 200:
            return False

        return ImpalaQueryLogParser(request.text).extract_profile(query)
