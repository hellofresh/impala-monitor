import click
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from impala_monitor.logger.logger import ImpalaLogger


@click.command()
@click.option('--nodes', help='List of nodes to track separated by a comma')
@click.option('--seconds', help='Every how many seconds should be executed',
              default=1)
@click.option('--kibana-node', help='To which Kibana should send the data')
@click.option('--kibana-port', help='To which Kibana port should '
                                      'connect', default=8125)
def monitor(nodes, seconds, kibana_node, kibana_port):
    monitor = ImpalaLogger(nodes.split(','), kibana_node, kibana_port)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(monitor.run, 'interval', seconds=seconds)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemError):
        print("Bye, bye Darling!")


if __name__ == '__main__':
    monitor()
