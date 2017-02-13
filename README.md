# impala-monitor

This a simple Python daemon to monitor your Impala nodes. 

### How it works

It's preferred to set up a supervisor configuration to make sure the daemon it's working all the time, in this case you can execute the script like this:

```
impala-monitor.py --nodes=127.0.0.1:25000,128.0.0.1:25000 --seconds=10 --graphite-node=my.graphite.node --graphite-port=8125 --graphite-prefix=department.{ENV}.impala --env=production
```

You can do `impala-monitor.py --help` to know more about each argument.
