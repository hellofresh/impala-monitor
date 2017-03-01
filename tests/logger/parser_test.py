import os
from unittest import TestCase
from datetime import datetime
from impala_monitor.logger.parser import ImpalaQueryLogParser


class HtmlLoader(object):
    def __init__(self):
        self.base_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'fixtures'
        )

    def load(self, file_name: str) -> str:
        full_path = os.path.join(
            self.base_path, file_name
        )

        with open(full_path, 'r') as buffer:
            html = buffer.read()
        buffer.close()

        return html


class ImpalaQueryLogParserTest(TestCase):
    def test_get_queries(self):
        html = HtmlLoader().load('impala_queries.html')
        parser = ImpalaQueryLogParser(html)

        queries = parser.queries

        self.assertEqual(20, len(queries))

        for query in queries:
            self.assertIsInstance(query['start_time'], datetime)
            self.assertIsInstance(query['end_time'], datetime)
