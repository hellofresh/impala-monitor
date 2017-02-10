import re


class ImpalaStats(object):
    ITEMS_TO_TRACK = [
        'admission-controller.*',
        'jvm.total.*',
        'impala.thrift-server.*',
        'impala-server.num-queries',
        'impala-server.num-queries-expired'
    ]

    def __init__(self, statsd):
        self._statsd = statsd

    def send(self, node, payload):
        for key in payload:
            for pattern in self.ITEMS_TO_TRACK:
                if re.match(pattern, key):

                    extended_key = "{}.{}".format(
                        node.replace(':25000', ''), key
                    )
                    self._statsd.gauge(extended_key, int(payload[key]))
