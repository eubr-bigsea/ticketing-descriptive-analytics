# Examples of descriptive analytics on ticketing data execution
---------------------------------------------------------------

This folder includes some examples of the execution graph of the COMPSs application.

1. *example-1-graph.pdf*: shows the graph when the application is executed with  
```
runcompss -m --pythonpath=$PWD $PWD/ticketing_analytics.py -d local -c $PWD/config.ini -q 0.9,0.9,0.55 bus-usage -s all
```

2. *example-2-graph.pdf*: shows the graph when the application is executed with 
```
runcompss -m --pythonpath=$PWD $PWD/ticketing_analytics.py -d local -c $PWD/config.ini passenger-usage -s all
```
