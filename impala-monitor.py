import click
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from impala_monitor.monitor import ImpalaMonitor


@click.command()
@click.option('--nodes', help='List of nodes to track separated by a comma')
@click.option('--seconds', help='Every how many seconds should be executed',
              default=1)
@click.option('--graphite-node', help='To which graphite should send the data')
@click.option('--graphite-port', help='To which graphite port should '
                                      'connect', default=8125)
@click.option('--graphite-prefix', help='Which prefix should have your metrics')
@click.option('--env', help='Whichi environment this is running', default='staging')
def monitor(nodes, seconds, graphite_node, graphite_port, graphite_prefix, env):
    graphite_prefix = graphite_prefix.replace("{ENV}", env)

    monitor = ImpalaMonitor(nodes, graphite_node, graphite_port,
                            graphite_prefix)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(monitor.run, 'interval', seconds=seconds)
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemError):
        print("Bye, bye Darling!")


if __name__ == '__main__':
    monitor()
