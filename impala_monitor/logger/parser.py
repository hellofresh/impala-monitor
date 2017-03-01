from bs4 import BeautifulSoup


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

            queries.append({
                'query': cells[2].get_text(),
                'query_type': query_type,
                'state': query_state,
                'fetched_rows': int(cells[8].get_text()),
                'user': cells[0].get_text(),
                'start_time': cells[4].get_text(),
                'end_time': cells[5].get_text(),
            })

        return queries
