import re
from bs4 import BeautifulSoup
from datetime import datetime


class Query(object):
    def __init__(self, query: dict):
        self.query = query

    def __getattr__(self, item):
        if item not in self.query:
            raise ValueError("Item {} does not exists".format(item))

        return self.query[item]


class ImpalaQueryLogParser(object):
    def __init__(self, html: str):
        self.soup = BeautifulSoup(html, 'html.parser')

    @property
    def queries(self) -> list:
        """
        From the HTML it should get the list of queries, with some metadata as:
        - Query ID
        - Number of fetched rows
        - Query
        :return: dict
        """
        tables = self.soup.findAll('table')

        # In our case we know it's the third table
        rows = tables[2].findAll('tr')

        queries = []

        for row in rows[1:len(rows)]:
            cells = row.findAll("td")
            query_type = cells[3].get_text()
            query_state = cells[7].get_text()

            if query_type not in ['QUERY'] or not query_state in ['FINISHED']:
                continue

            start_time = datetime.strptime(
                cells[4].get_text(), "%Y-%m-%d %H:%M:%S.%f000"
            )

            end_time = datetime.strptime(
                cells[5].get_text(), "%Y-%m-%d %H:%M:%S.%f000"
            )

            execution_time = end_time - start_time

            query_id = self.extract_query_id(
                cells[9].find('a', href=True).get('href')
            )

            queries.append(Query({
                'query': cells[2].get_text(),
                'query_type': query_type,
                'state': query_state,
                'fetched_rows': int(cells[8].get_text()),
                'user': cells[0].get_text(),
                'start_time': start_time,
                'end_time': end_time,
                'execution_time': execution_time,
                'query_id': query_id
            }))

        return queries

    def extract_profile(self, query: Query) -> Query:
        profile = self.soup.findAll('div', {'class': 'container'})[1].find(
            'pre').get_text()

        memory_allocated = re.search('Memory=([0-9\.GB]+)', profile).group(1)
        vcores_allocated = re.search('VCores=([0-9]+)', profile).group(1)

        query = query.query
        query['memory_allocated'] = memory_allocated
        query['vcores_allocated'] = vcores_allocated

        return Query(query)

    @staticmethod
    def extract_query_id(link: str) -> str:
        """
        Given a link it extracts the query id
        :param link:
        :return:
        """
        regex = 'query_id=([a-z0-9\:]+)'

        matches = re.search(regex, link)

        if not matches:
            return None

        return matches.group(1)