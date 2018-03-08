# Descriptive analytics on ticketing data
-----------------------------------------

Python application based on Ophidia and COMPSs to compute statistical descriptive information from bus data.

# Requirements

The application requires Python 2.7 and the following Python modules: numpy, pandas and netCDF4 (and optionally matplotlib). Additionally, in order to run the application a COMPSs installation (with PyCOMPSs) and the latest PyOphidia version are required. Additionally, a running Ophidia endpoint is required. 

For info about COMPSs and the installation procedure, see the [COMP Superscalar web page](https://www.bsc.es/research-and-development/software-and-apps/software-list/comp-superscalar).

For info about Ophidia and the installation procedure, see the [Ophidia framework web page](http://ophidia.cmcc.it).

# Run the application

To run the application: 

1. update the *config.ini* file with the proper credentials to access the Ophidia endpoint, the folders to data and the other configuration parameters

2. launch the application, as in the following:
```
runcompss -m --pythonpath=$PWD  $PWD/ticketing_analytics.py bus-usage -c $PWD/config.ini -s all
``` 

To get the full list of options available use -h or --help command line option.
