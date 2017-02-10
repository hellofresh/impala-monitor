import click
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from impala_monitor.monitor import ImpalaMonitor


@click.command()
@click.option('--nodes', help='List of nodes to track separated by a comma')
@click.option('--seconds', help='Every how many seconds should be executed',
              default=1)
@click.option('--graphite-node', help='To which graphite should send the data')
@click.option('--env', help='Whichi environment this is running', default='staging')
def monitor(nodes, seconds, graphite_node, env):
    monitor = ImpalaMonitor(nodes, graphite_node, env)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(monitor.run, 'interval', seconds=seconds)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemError):
        print("Bye, bye Darling!")


if __name__ == '__main__':
    monitor()
