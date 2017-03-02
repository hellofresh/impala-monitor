import click
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from impala_monitor.logger.logger import ImpalaLogger


@click.command()
@click.option('--nodes', help='List of nodes to track separated by a comma')
@click.option('--seconds', help='Every how many seconds should be executed',
              default=1)
@click.option('--elastic-node', help='To which Elastic should send the data')
@click.option('--elastic-port', help='To which Elastic port should '
                                      'connect', default=9200)
def monitor(nodes, seconds, elastic_node, elastic_port):
    monitor = ImpalaLogger(nodes.split(','), elastic_node, elastic_port)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(monitor.run, 'interval', seconds=seconds)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemError):
        print("Bye, bye Darling!")


if __name__ == '__main__':
    monitor()
