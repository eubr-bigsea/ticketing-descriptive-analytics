import sys, time, json, numpy, calendar, datetime, os
from PyOphidia import cube, client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common

METRICS_BUS = ['MIN', 'MAX', 'AVG', 'SUM']
METRICS_USER = ['MIN', 'MAX', 'COUNT', 'SUM']

#Functions for Ophidia aggregations
def simpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port, mode):
	if mode == 'compss':
		from compss_functions import compssSimpleAggregation
		return compssSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalSimpleAggregation
		return internalSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port)

def reducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, mode):
	if mode == 'compss':
		from compss_functions import compssReducedAggregation
		return compssReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalReducedAggregation
		return internalReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)

def totalAggregation(startCube, metric, parallelNcores, user, pwd, host, port, mode):
	if mode == 'compss':
		from compss_functions import compssTotalAggregation
		return compssTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalTotalAggregation
		return internalTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port)

def totalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port, mode):
	if mode == 'compss':
		from compss_functions import compssTotalHourlyAggregation
		return compssTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalTotalHourlyAggregation
		return internalTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port)


def buildValues(aggregation, cubeList, day):

	dataList = []
	for c in cubeList:
		#Extract data
		data = c.export_array(show_time='yes')
		dataList.append(data)

	#Get dimension and measure values
	mainDimData = None
	dateData = None
	measureData = []
	tmpDat = dataList[0]

	if not tmpDat:
		exit("ERROR: Missing datacube")

	for k in tmpDat['dimension']:
		if aggregation == 'weekdays-peakhours' or aggregation == 'weekdaysets-peakhours':
			mainDimData = ""
		elif aggregation == 'weekly-usage' or aggregation == 'monthly-usage':
			"""import random
			random.seed()"""
			if(k['name'] == 'cod_passenger'):
				mainDimData = k['values']
				"""#Randomly generate ZIP codes
				for i, v in enumerate(mainDimData):
					mainDimData[i] = str(random.randint(0, 9999)*10).zfill(5) + "-" + str(random.randint(0, 99)*10).zfill(3)"""
				#Convert timestamp to datatime string (when value is available)
				for i, v in enumerate(mainDimData):
					if mainDimData[i] != 0:
						tmp = str(mainDimData[i])
						if mainDimData[i] > 0:
							mainDimData[i] = [datetime.datetime.fromtimestamp(int(tmp[:-1])).strftime("%d.%m.%Y"),"F" if int(tmp[-1:]) == 1 else "M"]
						else:
							mainDimData[i] = [(datetime.datetime(1970, 1, 2) + datetime.timedelta(seconds=int(tmp[:-1]))).strftime("%d.%m.%Y"),"F" if int(tmp[-1:]) == 1 else "M"]
					else:		
						mainDimData[i] = [numpy.nan, numpy.nan]
		else:
			if(k['name'] == 'cod_linha'):
				mainDimData = k['values']
		if aggregation == 'weekdays-lines' or aggregation == 'weekdaysets-lines':
			dateData = [day]
		else:
			if(k['name'] == 'time'):
				if aggregation == 'hourly-lines':
					dateData = k['values']
				elif aggregation == 'daily-lines':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y-%m-%d") for m in k['values'] ]
				elif aggregation == 'weekly-lines':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y") + " W" + str(datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().isocalendar()[1]) for m in k['values'] ]
				elif aggregation == 'monthly-lines':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y-%m") for m in k['values'] ]
				elif aggregation == 'weekdays-peakhours' or aggregation == 'weekdaysets-peakhours':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").time().strftime("%H") for m in k['values'] ]
					for x,d in enumerate(dateData):
						if d == "00":
							dateData[x] = "24"

					dateData = [day + " " + str(int(m)-1) + "-" + str(int(m)) for m in dateData ]
				elif aggregation == 'weekdays-hourly-lines' or aggregation == 'weekdaysets-hourly-lines':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").time().strftime("%H") for m in k['values'] ]
					for x,d in enumerate(dateData):
						if d == "00":
							dateData[x] = "24"

					dateData = [day + " " + str(int(m)-1) + "-" + str(int(m)) for m in dateData ]
				elif aggregation == 'weekly-usage':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y") + " W" + str(datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().isocalendar()[1]) for m in k['values'] ]
				elif aggregation == 'monthly-usage':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y-%m") for m in k['values'] ]

	for d in dataList:
		for k in d['measure']:
			if(k['name'] == 'passengers' or k['name'] == 'usage'):
				measureData.append(k['values'])

	if mainDimData == None or dateData == None or not measureData:
		exit("ERROR: Variables not found")

	return mainDimData, dateData, measureData


def basicLineAggregation(parallelNcores, singleNcores, startCube, format, aggregation, outputFolder, user, pwd, host, port, mode):
	cubeList = [0 for m in METRICS_BUS]
	if aggregation == 'weekly-lines':
		for i, m in enumerate(METRICS_BUS):
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'w', parallelNcores, user, pwd, host, port, mode)
	elif aggregation == 'monthly-lines':
		for i, m in enumerate(METRICS_BUS):
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'M', parallelNcores, user, pwd, host, port, mode)
	elif aggregation == 'daily-lines':
		for i, m in enumerate(METRICS_BUS):
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'd', parallelNcores, user, pwd, host, port, mode)
	elif aggregation == 'hourly-lines':
		for i, m in enumerate(METRICS_BUS):
			cubeList[i] = simpleAggregation(startCube, m.lower(), parallelNcores, user, pwd, host, port, mode)

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	#Get dimension and measure values
	codLinhaData = None
	dateData = None
	passengerData = []
	codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList, None)

	#Build json file and array for plot
	if format == 'json':
		outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
	else:
		outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)

	return outFile

def basicPassengerAggregation(parallelNcores, singleNcores, startCube, format, aggregation, outputFolder, user, pwd, host, port, mode):
	cubeList = [0 for m in METRICS_USER]
	if aggregation == 'weekly-usage':
		for i, m in enumerate(METRICS_USER):
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'w', parallelNcores, user, pwd, host, port, mode)
	elif aggregation == 'monthly-usage':
		for i, m in enumerate(METRICS_USER):
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'M', parallelNcores, user, pwd, host, port, mode)

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	#Get dimension and measure values
	passengerData = None
	dateData = None
	usageData = []
	passengerData, dateData, usageData = buildValues(aggregation, cubeList, None)

	#Build json file and array for plot
	if format == 'json':
		outFile = common.createJSONFilePassengerUsage(outputFolder, aggregation, usageData, passengerData, dateData, 'w', 0)
	else:
		outFile = common.createCSVFilePassengerUsage(outputFolder, aggregation, usageData, passengerData, dateData, 'w', 0)

	return outFile

def weekdayLinesAggregation(parallelNcores, singleNcores, aggregation, startCube, startDate, numDays, format, outputFolder, user, pwd, host, port, mode):
	#weekdays array
	if aggregation == 'weekdays-hourly-lines': 
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
		weekDaysId = []
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	cubeList = [[0 for m in METRICS_BUS] for k in weekDays]
	for idx, day in enumerate(weekDays):
		#Build filter set 
		filter_list = ""
		if aggregation == 'weekdays-hourly-lines': 
			filter_list = common.buildSubsetFilter(startDate, numDays, idx+1) 
		else:
			for j in weekDaysId[idx]:
				filter_list = filter_list + common.buildSubsetFilter(startDate, numDays, j) + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			exit("ERROR: Subset filter creation")

		#Extract relevant days
		subsettedCube = startCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='no',ncores=singleNcores)  

		for i, m in enumerate(METRICS_BUS):
			cubeList[idx][i] = totalHourlyAggregation(subsettedCube, m.lower(), parallelNcores, user, pwd, host, port, mode)

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	for idx, day in enumerate(weekDays):
		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList[idx], day)

		#Build json file and array for plot
		if format == 'json':
			outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if idx == 0 else 'a', 0)
		else:
			outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData,  'w' if idx == 0 else 'a', 0)

	return outFile

def weekdayLinesTotalAggregation(parallelNcores, singleNcores, aggregation, startCube, startDate, numDays, format, outputFolder, user, pwd, host, port, mode):
	#weekdays array
	if aggregation == 'weekdays-lines': 
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
		weekDaysId = []
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	#Aggregate at day level once
	reducedCube = startCube.reduce2(dim='time',concept_level='d',operation='sum',ncores=parallelNcores)

	cubeList = [[0 for m in METRICS_BUS] for k in weekDays]
	for idx, day in enumerate(weekDays):
		#Build filter set 
		filter_list = ""
		if aggregation == 'weekdays-lines': 
			filter_list = common.buildSubsetFilter(startDate, numDays, idx+1) 
		else:
			for j in weekDaysId[idx]:
				filter_list = filter_list + common.buildSubsetFilter(startDate, numDays, j) + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			exit("ERROR: Subset filter creation")

		#Extract relevant days
		subsettedCube = reducedCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='no',ncores=singleNcores)  

		for i, m in enumerate(METRICS_BUS):
			cubeList[idx][i] = totalHourlyAggregation(subsettedCube, m.lower(), parallelNcores, user, pwd, host, port, mode)

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	for idx, day in enumerate(weekDays):
		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList[idx], day)

		#Build json file and array for plot
		if format == 'json':
			outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if idx == 0 else 'a', 0)
		else:
			outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if idx == 0 else 'a', 0)

	return outFile

def peakhourAggregation(parallelNcores, singleNcores, aggregation, startCube, startDate, numDays, format, outputFolder, user, pwd, host, port, mode):
	#weekdays array
	if aggregation == 'weekdays-peakhours': 
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
		weekDaysId = []
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	#Sum values on all lines once
	mergedCube = startCube.merge(nmerge=0,ncores=1)
	aggregatedCube = mergedCube.aggregate(group_size='all',operation='sum',ncores=1)

	cubeList = [[0 for m in METRICS_BUS] for k in weekDays]
	for idx, day in enumerate(weekDays):
		filter_list = ""
		#Build filter set 
		if aggregation == 'weekdays-peakhours': 
			filter_list = common.buildSubsetFilter(startDate, numDays, idx+1) 
		else:
			for j in weekDaysId[idx]:
				filter_list = filter_list + common.buildSubsetFilter(startDate, numDays, j) + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			exit("ERROR: Subset filter creation")

		#Extract relevant days
		subsettedCube = aggregatedCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='no',ncores=singleNcores)  

		#cubeList = [0 for m in METRICS_BUS]
		for i, m in enumerate(METRICS_BUS):
			cubeList[idx][i] = totalHourlyAggregation(subsettedCube, m.lower(), parallelNcores, user, pwd, host, port, mode)

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	for idx, day in enumerate(weekDays):
		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList[idx], day)

		#Build json file and array for plot
		if format == 'json':
			outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, None , dateData, 'w' if idx == 0 else 'a', 1)
		else:
			outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, None, dateData, 'w' if idx == 0 else 'a', 1)

	return outFile


def computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, processing, format, outputFolder, procType, mode):

	if procType == "busUsage":
		measure = "passengers"
	elif procType == "passengerUsage":
		measure = "usage"
	else:
		raise RuntimeError("Type of processing not recognized")

	#Initialize
	sys.stdout = open(os.devnull, 'w')
	if user is "__TOKEN__":
		cube.Cube.setclient(token=password, server=hostname, port=port)
	else:
		cube.Cube.setclient(username=user, password=password, server=hostname, port=port)
	sys.stdout = sys.__stdout__; 

	#Get Historical cube PID from metadata
	cube.Cube.search(container_filter='bigsea',metadata_key_filter='datacube_name',metadata_value_filter='historical_'+measure,display=False)
	data = json.loads(cube.Cube.client.last_response)
	if not data['response'][0]['objcontent'][0]['rowvalues']:
		exit("ERROR: Historical datacube not found")
	else:
		historicalCubePid = data['response'][0]['objcontent'][0]['rowvalues'][0][0]

	historicalCube = cube.Cube(pid=historicalCubePid)
	#Get Historical start/end date from metadata

	historicalCube.metadata(metadata_key='start_date',display=False)
	data = json.loads(cube.Cube.client.last_response)
	if not data['response'][1]['objcontent'][0]['rowvalues']:
		exit("ERROR: Historical datacube not found")
	else:
		startDate = data['response'][1]['objcontent'][0]['rowvalues'][0][4]
		startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")

	historicalCube.metadata(metadata_key='end_date',display=False)
	data = json.loads(cube.Cube.client.last_response)
	if not data['response'][1]['objcontent'][0]['rowvalues']:
		exit("ERROR: Historical datacube not found")
	else:
		endDate = data['response'][1]['objcontent'][0]['rowvalues'][0][4]
		endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

	aggregatedCube = historicalCube.aggregate(group_size='all',operation='sum',ncores=singleNcores)

	#Subset on weekdays (monday is 0)
	numDays = (endDate - startDate).days + 1
	startDay = startDate.isoweekday()

	if processing == 'hourly-lines' or processing == 'daily-lines' or processing == 'weekly-lines' or processing == 'monthly-lines':
		#Description: hourly/daily/weekly/monthly aggregated stats for each bus line and time range
		outFile = basicLineAggregation(parallelNcores, singleNcores, aggregatedCube, format, processing, outputFolder, user, password, hostname, port, mode)
	elif processing == 'weekdays-hourly-lines' or processing == 'weekdaysets-hourly-lines':
		#Description: hourly aggregated stats for each bus line and weekday or set of weekdays
		outFile = weekdayLinesAggregation(parallelNcores, singleNcores, processing, aggregatedCube, startDate, numDays, format, outputFolder, user, password, hostname, port, mode)
	elif processing == 'weekdays-lines' or processing == 'weekdaysets-lines':
		#Description: daily aggregated stats for each bus line and weekday or set of weekdays
		outFile = weekdayLinesTotalAggregation(parallelNcores, singleNcores, processing, aggregatedCube, startDate, numDays, format, outputFolder, user, password, hostname, port, mode)
	elif processing == 'weekdays-peakhours' or processing == 'weekdaysets-peakhours':
		#Description: hourly aggregated stats for each weekday or set of weekdays (on all lines)
		outFile = peakhourAggregation(parallelNcores, singleNcores, processing, aggregatedCube, startDate, numDays, format, outputFolder, user, password, hostname, port, mode)
	elif processing == 'weekly-usage' or processing == 'monthly-usage':
		#Description: monthly aggregated stats for each bus user
		outFile = basicPassengerAggregation(parallelNcores, singleNcores, aggregatedCube, format, processing, outputFolder, user, password, hostname, port, mode)
	else:
		print("Aggregation not recognized")

	#Remove tmp cubes
	cube.Cube.client.submit("oph_delete cube=[container=bigsea;level=1|2|3|4|5|6]")
	#cube.Cube.deletecontainer(container=str(sample_container), delete_type='physical', hidden='no')

	return outFile
