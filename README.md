# impala-monitor

This a simple Python daemon to monitor your Impala nodes. 

### How it works

It's preferred to set up a supervisor configuration to make sure the daemon it's working all the time, in this case you can execute the script like this:

```
impala-monitor.py --nodes=127.0.0.1:25000,128.0.0.1:25000 --seconds=10 --graphite-node=my.graphite.node --graphite-port=8125 --graphite-prefix=department.{ENV}.impala --env=production
```

You can do `impala-monitor.py --help` to know more about each argument.

License
-------

    Copyright (C) 2017 HelloFresh SE

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

