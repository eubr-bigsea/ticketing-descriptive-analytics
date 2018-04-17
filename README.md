# Descriptive analytics on ticketing data
-----------------------------------------

Python application based on Ophidia and COMPSs to compute statistical descriptive information from bus data.

# Requirements

The application requires Python 2.7 and the following Python modules: numpy, pandas and netCDF4 (and optionally matplotlib). Additionally, in order to run the application a COMPSs installation (with PyCOMPSs) and the latest PyOphidia version are required. Additionally, a running Ophidia endpoint is required. 

For info about COMPSs and the installation procedure, see the [COMP Superscalar web page](https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar).

For info about Ophidia and the installation procedure, see the [Ophidia framework web page](http://ophidia.cmcc.it).

# Run the application

To run the application: 

1. update the *config.ini* file with the proper credentials to access the Ophidia endpoint, the folders for input/output data, the path to data privacy tool and the other configuration parameters

2. launch the application, as in the following:
```
runcompss -m --pythonpath=$PWD  $PWD/ticketing_analytics.py bus-usage -c $PWD/config.ini -s all
``` 

To get the full list of options available use -h or --help command line option. The *examples* folder provides some examples of the execution graph of the application as well as the command to run it.

# Type of metrics computed by the application

* *Bus line* stats: statistics related to number of passengers for each bus line (or whole network) and time period. 
	* stats for each bus line (a record is provided only for the time periods in which at least one passenger used a line). Temporal aggregations available include: group of weekdays (e.g. all Saturday/Sunday, all Monday/Friday and all Tuesday/Wednesday/Thursday), weekday (e.g. all Mondays, all Tuesdays, etc.), hour of group of weekdays (e.g. Saturday/Sunday from 0-1, Saturday/Sunday from 1-2, etc.), hour of weekday (e.g. Monday from 0-1, Monday from 1-2, etc.), month, week (the ISO week number is used), day or hour
	* stats aggregated over the whole bus network. Temporal aggregations available include: hour of group of weekdays (e.g. Saturday/Sunday from 0-1, Saturday/Sunday from 1-2, etc.) or hour of weekday (e.g. Monday from 0-1, Monday from 1-2, etc.)
* *Bus passenger* stats: statistics related to the number of times a single passenger took any bus. Temporal aggregations available include: week or month
* *Bus stop* stats: statistics related to number of passengers boarding for each bus stop. A record is provided only for the time periods in which at least one passenger boarded from a bus stop. Temporal aggregations available include: group of weekdays, weekday, hour of weekday, hour of group of weekdays, month, week, day or hour

