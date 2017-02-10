import re


class ImpalaStats(object):
    items_to_track = [
        'admission-controller.*'
    ]

    def __init__(self, statsd):
        self._statsd = statsd

    def send(self, node, payload):
        for key in payload:
            for pattern in self.items_to_track:
                if re.match(pattern, key):
                    extended_key = "{}.{}".format(
                        node.replace(':25000', ''), key
                    )

                    self._statsd.gauge(extended_key, int(payload[key]))
