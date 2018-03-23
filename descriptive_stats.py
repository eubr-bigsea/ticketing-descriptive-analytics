import sys, time, json, numpy, calendar, datetime, os
from PyOphidia import cube, client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common

METRICS_BUS = ['MIN', 'MAX', 'AVG', 'SUM']
METRICS_USER = ['MIN', 'MAX', 'COUNT', 'SUM']

import timeit
import logging
import inspect

#Functions for Ophidia aggregations
def simpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port, mode, logFlag):
	if mode == 'compss':
		from compss_functions import compssSimpleAggregation
		return compssSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalSimpleAggregation
		return internalSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port, logFlag)

def reducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, mode, logFlag):
	if mode == 'compss':
		from compss_functions import compssReducedAggregation
		return compssReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalReducedAggregation
		return internalReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, logFlag)

def verticalAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, mode, logFlag):
	if mode == 'compss':
		from compss_functions import compssVerticalAggregation
		return compssVerticalAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalVerticalAggregation
		return internalVerticalAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, logFlag)

def totalAggregation(startCube, metric, parallelNcores, user, pwd, host, port, mode, logFlag):
	if mode == 'compss':
		from compss_functions import compssTotalAggregation
		return compssTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalTotalAggregation
		return internalTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port, logFlag)

def totalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port, mode, logFlag):
	if mode == 'compss':
		from compss_functions import compssTotalHourlyAggregation
		return compssTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
	else:
		from internal_functions import internalTotalHourlyAggregation
		return internalTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port, logFlag)


def buildValues(aggregation, dataList, day):

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
			elif(k['name'] == 'bus_stop'):
				mainDimData = k['values']
		if aggregation == 'weekdays-lines' or aggregation == 'weekdaysets-lines' or aggregation == 'weekdays-stops' or aggregation == 'weekdaysets-stops':
			dateData = [day]
		else:
			if(k['name'] == 'time'):
				if aggregation == 'hourly-lines' or  aggregation == 'hourly-stops':
					dateData = k['values']
				elif aggregation == 'daily-lines' or aggregation == 'daily-stops':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y-%m-%d") for m in k['values'] ]
				elif aggregation == 'weekly-lines' or aggregation == 'weekly-stops':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y") + " W" + str(datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().isocalendar()[1]) for m in k['values'] ]
				elif aggregation == 'monthly-lines' or aggregation == 'monthly-stops':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").date().strftime("%Y-%m") for m in k['values'] ]
				elif aggregation == 'weekdays-peakhours' or aggregation == 'weekdaysets-peakhours':
					dateData = [datetime.datetime.strptime(m, "%Y-%m-%d %H:%M:%S").time().strftime("%H") for m in k['values'] ]
					for x,d in enumerate(dateData):
						if d == "00":
							dateData[x] = "24"

					dateData = [day + " " + str(int(m)-1) + "-" + str(int(m)) for m in dateData ]
				elif aggregation == 'weekdays-hourly-lines' or aggregation == 'weekdaysets-hourly-lines' or aggregation == 'weekdays-hourly-stops' or aggregation == 'weekdaysets-hourly-stops':
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


def basicLineAggregation(parallelNcores, singleNcores, startCube, format, aggregation, outputFolder, user, pwd, host, port, mode, logFlag):
	cubeList = [0 for m in METRICS_BUS]
	if aggregation == 'weekly-lines' or aggregation == 'weekly-stops':
		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'w', parallelNcores, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] BASIC LINE/STOP WEEK %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), m, str(end_time))

	elif aggregation == 'monthly-lines' or aggregation == 'monthly-stops':
		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'M', parallelNcores, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] BASIC LINE/STOP MONTH  %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), m, str(end_time))

	elif aggregation == 'daily-lines' or aggregation == 'daily-stops':
		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[i] = reducedAggregation(startCube, m.lower(), 'd', parallelNcores, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] BASIC LINE/STOP DAY %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), m, str(end_time))

	elif aggregation == 'hourly-lines' or aggregation == 'hourly-stops':
		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[i] = simpleAggregation(startCube, m.lower(), parallelNcores, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] BASIC LINE/STOP HOUR %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), m, str(end_time))

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	#Get dimension and measure values
	codLinhaData = None
	dateData = None
	passengerData = []
	codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList, None)

	#Build json file and array for plot
	if "stops" in aggregation:
		if format == 'json':
			outFile = common.createJSONFileBusStops(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
		else:
			outFile = common.createCSVFileBusStops(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
	else:
		if format == 'json':
			outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
		else:
			outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)

	return outFile


def basicPassengerAggregation(parallelNcores, singleNcores, startCube, format, aggregation, outputFolder, user, pwd, host, port, mode, logFlag):
	cubeList = [0 for m in METRICS_USER]
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	mergedCube = startCube.merge(nmerge=0,ncores=1)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] MERGE execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	if aggregation == 'weekly-usage':
		for i, m in enumerate(METRICS_USER):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[i] = verticalAggregation(mergedCube, m.lower(), 'w', 1, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] USER WEEKLY USAGE %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), m, str(end_time))
	elif aggregation == 'monthly-usage':
		for i, m in enumerate(METRICS_USER):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[i] = verticalAggregation(mergedCube, m.lower(), 'M', 1, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] USER MONTHLY USAGE %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), m, str(end_time))

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

def weekdayLinesAggregation(parallelNcores, singleNcores, aggregation, startCube, startDate, numDays, format, outputFolder, user, pwd, host, port, mode, logFlag):
	#weekdays array
	if aggregation == 'weekdays-hourly-lines' or aggregation == 'weekdays-hourly-stops':
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
		weekDaysId = []
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	cubeList = [[0 for m in METRICS_BUS] for k in weekDays]
	for idx, day in enumerate(weekDays):
		#Build filter set
		filter_list = ""
		if aggregation == 'weekdays-hourly-lines' or aggregation == 'weekdays-hourly-stops':
			filter_list = common.buildSubsetFilter(startDate, numDays, idx+1)
		else:
			for j in weekDaysId[idx]:
				tmp_filter_list = common.buildSubsetFilter(startDate, numDays, j)
				if not tmp_filter_list:
					continue
				filter_list = filter_list + tmp_filter_list + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			continue

		#Extract relevant days
		if logFlag == True:
			frame = inspect.getframeinfo(inspect.currentframe())
			start_time = timeit.default_timer()
		subsettedCube = startCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='no',ncores=singleNcores)
		if logFlag == True:
			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] SUBSET %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), day, str(end_time))

		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[idx][i] = totalHourlyAggregation(subsettedCube, m.lower(), parallelNcores, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] HOURLY LINES/STOPS %s %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), day, m, str(end_time))

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	first_day = True
	for idx, day in enumerate(weekDays):
		if cubeList[idx][0] == 0:
			continue
		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList[idx], day)

		#Build json file and array for plot
		if "stops" in aggregation:
			if format == 'json':
				outFile = common.createJSONFileBusStops(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if first_day == True else 'a', 0)
			else:
				outFile = common.createCSVFileBusStops(outputFolder, aggregation, passengerData, codLinhaData, dateData,  'w' if first_day == True else 'a', 0)
		else:
			if format == 'json':
				outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if first_day == True else 'a', 0)
			else:
				outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData,  'w' if first_day == True else 'a', 0)

		first_day = False

	return outFile

def weekdayLinesTotalAggregation(parallelNcores, singleNcores, aggregation, startCube, startDate, numDays, format, outputFolder, user, pwd, host, port, mode, logFlag):
	#weekdays array
	if aggregation == 'weekdays-lines' or aggregation == 'weekdays-stops':
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
		weekDaysId = []
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	#Aggregate at day level once
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	reducedCube = startCube.reduce2(dim='time',concept_level='d',operation='sum',ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] REDUCE2 execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	cubeList = [[0 for m in METRICS_BUS] for k in weekDays]
	for idx, day in enumerate(weekDays):
		#Build filter set
		filter_list = ""
		if aggregation == 'weekdays-lines' or aggregation == 'weekdays-stops':
			filter_list = common.buildSubsetFilter(startDate, numDays, idx+1)
		else:
			for j in weekDaysId[idx]:
				tmp_filter_list = common.buildSubsetFilter(startDate, numDays, j)
				if not tmp_filter_list:
					continue
				filter_list = filter_list + tmp_filter_list + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			continue

		#Extract relevant days
		if logFlag == True:
			start_time = timeit.default_timer()
		subsettedCube = reducedCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='no',ncores=singleNcores)
		if logFlag == True:
			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] SUBSET %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), day, str(end_time))

		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[idx][i] = totalAggregation(subsettedCube, m.lower(), parallelNcores, user, pwd, host, port, mode, logFlag)
			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] DAILY LINES/STOPS %s %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), day, m, str(end_time))

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	first_day = True
	for idx, day in enumerate(weekDays):
		if cubeList[idx][0] == 0:
			continue
		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList[idx], day)

		#Build json file and array for plot
		if "stops" in aggregation:
			if format == 'json':
				outFile = common.createJSONFileBusStops(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if first_day == True else 'a', 0)
			else:
				outFile = common.createCSVFileBusStops(outputFolder, aggregation, passengerData, codLinhaData, dateData,  'w' if first_day == True else 'a', 0)
		else:
			if format == 'json':
				outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w' if first_day == True else 'a', 0)
			else:
				outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, codLinhaData, dateData,  'w' if first_day == True else 'a', 0)

		first_day = False

	return outFile

def peakhourAggregation(parallelNcores, singleNcores, aggregation, startCube, startDate, numDays, format, outputFolder, user, pwd, host, port, mode, logFlag):
	#weekdays array
	if aggregation == 'weekdays-peakhours':
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
		weekDaysId = []
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	#Sum values on all lines once
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	mergedCube = startCube.merge(nmerge=0,ncores=1)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] MERGE execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	aggregatedCube = mergedCube.aggregate(group_size='all',operation='sum',ncores=1)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] AGGREGATE execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	cubeList = [[0 for m in METRICS_BUS] for k in weekDays]
	for idx, day in enumerate(weekDays):
		filter_list = ""
		#Build filter set
		if aggregation == 'weekdays-peakhours':
			filter_list = common.buildSubsetFilter(startDate, numDays, idx+1)
		else:
			for j in weekDaysId[idx]:
				tmp_filter_list = common.buildSubsetFilter(startDate, numDays, j)
				if not tmp_filter_list:
					continue
				filter_list = filter_list + tmp_filter_list + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			continue

		#Extract relevant days
		if logFlag == True:
			start_time = timeit.default_timer()
		subsettedCube = aggregatedCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='no',ncores=singleNcores)
		if logFlag == True:
			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] SUBSET %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), day, str(end_time))

		#cubeList = [0 for m in METRICS_BUS]
		for i, m in enumerate(METRICS_BUS):
			if logFlag == True:
				frame = inspect.getframeinfo(inspect.currentframe())
				start_time = timeit.default_timer()
			cubeList[idx][i] = totalHourlyAggregation(subsettedCube, m.lower(), parallelNcores, user, pwd, host, port, mode, logFlag)

			if logFlag == True:
				end_time = timeit.default_timer() - start_time
				logging.debug('[%s] [%s - %s] PEAKHOURS %s %s execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), day, m, str(end_time))

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		cubeList = compss_wait_on(cubeList)

	first_day = True
	for idx, day in enumerate(weekDays):
		if cubeList[idx][0] == 0:
			continue
		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList[idx], day)

		#Build json file and array for plot
		if format == 'json':
			outFile = common.createJSONFileBusUsage(outputFolder, aggregation, passengerData, None , dateData, 'w' if first_day == True else 'a', 1)
		else:
			outFile = common.createCSVFileBusUsage(outputFolder, aggregation, passengerData, None, dateData, 'w' if first_day == True else 'a', 1)

		first_day = False

	return outFile


def applyFilters(singleNcores, user, password, hostname, port, procType, cubePid, dq_filters, logFlag):

	if procType == "busUsage":
		measure = "passengers"
		int_dim = "cod_veiculo"
	elif procType == "passengerUsage":
		measure = "usage"
		int_dim = "cod_linha"
	elif procType == "busStops":
		measure = "passengers"
		int_dim = "cod_linha"
	else:
		raise RuntimeError("Type of processing not recognized")

	if len(cubePid) < 1:
		raise RuntimeError("Input data cube is missing")

	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time_begin = timeit.default_timer()

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
		for c in data['response'][0]['objcontent'][0]['rowvalues']:
			if c[0] == cubePid[0]:
				historicalCubePid = c[0]
				break

	if not historicalCubePid:
		exit("ERROR: Historical datacube not found")

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

	if len(cubePid) == 4:
		#Apply data quality filters
		if logFlag == True:
			frame = inspect.getframeinfo(inspect.currentframe())
			start_time = timeit.default_timer()
		for i in range(len(cubePid[1:])):
			#Check data quality filters
			if dq_filters[i] is not None:
				#Apply filer
				dqcube = cube.Cube(pid=cubePid[1+i])
				dqcube = dqcube.apply(query="oph_predicate('oph_float','oph_float',measure,'x-"+str(dq_filters[i])+"','>=0','1','NaN')", ncores=singleNcores)
				newHistoricalCube = historicalCube.intercube(cube2=dqcube.pid, operation='mask', ncores=singleNcores)
				dqcube.delete()
				if i != 0:
					historicalCube.delete()
				historicalCube = newHistoricalCube

		if logFlag == True:
			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] APPLY DQ filters execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	aggregatedCube = historicalCube.aggregate2(concept_level='A',operation='sum',dim=int_dim,ncores=singleNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] AGGREGATE execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	if procType == "busStops":
		#Filter occurences with value set to zero
		if logFlag == True:
			frame = inspect.getframeinfo(inspect.currentframe())
			start_time = timeit.default_timer()
		aggregatedNewCube = aggregatedCube.apply(query="oph_predicate('oph_float','oph_float',measure,'x-0.1','>0','x','NaN')", ncores=singleNcores)
		aggregatedCube.delete()
		aggregatedCube = aggregatedNewCube
		if logFlag == True:
			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] APPLY ZERO FILTER execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))


	return (aggregatedCube, endDate, startDate)


def computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, processing, format, outputFolder, procType, mode, logFlag):

	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time_begin = timeit.default_timer()

	#Initialize
	sys.stdout = open(os.devnull, 'w')
	if user is "__TOKEN__":
		cube.Cube.setclient(token=password, server=hostname, port=port)
	else:
		cube.Cube.setclient(username=user, password=password, server=hostname, port=port)
	sys.stdout = sys.__stdout__;

	#Subset on weekdays (monday is 0)
	numDays = (endDate - startDate).days + 1
	startDay = startDate.isoweekday()

	if processing == 'hourly-lines' or processing == 'daily-lines' or processing == 'weekly-lines' or processing == 'monthly-lines' or processing == 'hourly-stops' or processing == 'daily-stops' or processing == 'weekly-stops' or processing == 'monthly-stops':
		#Description: hourly/daily/weekly/monthly aggregated stats for each bus line/stop and time range
		outFile = basicLineAggregation(parallelNcores, singleNcores, aggregatedCube, format, processing, outputFolder, user, password, hostname, port, mode, logFlag)
	elif processing == 'weekdays-hourly-lines' or processing == 'weekdaysets-hourly-lines' or processing == 'weekdays-hourly-stops' or processing == 'weekdaysets-hourly-stops':
		#Description: hourly aggregated stats for each bus line and weekday or set of weekdays
		outFile = weekdayLinesAggregation(parallelNcores, singleNcores, processing, aggregatedCube, startDate, numDays, format, outputFolder, user, password, hostname, port, mode, logFlag)
	elif processing == 'weekdays-lines' or processing == 'weekdaysets-lines' or processing == 'weekdays-stops' or processing == 'weekdaysets-stops':
		#Description: daily aggregated stats for each bus line and weekday or set of weekdays
		outFile = weekdayLinesTotalAggregation(parallelNcores, singleNcores, processing, aggregatedCube, startDate, numDays, format, outputFolder, user, password, hostname, port, mode, logFlag)
	elif processing == 'weekdays-peakhours' or processing == 'weekdaysets-peakhours':
		#Description: hourly aggregated stats for each weekday or set of weekdays (on all lines)
		outFile = peakhourAggregation(parallelNcores, singleNcores, processing, aggregatedCube, startDate, numDays, format, outputFolder, user, password, hostname, port, mode, logFlag)
	elif processing == 'weekly-usage' or processing == 'monthly-usage':
		#Description: monthly aggregated stats for each bus user
		outFile = basicPassengerAggregation(parallelNcores, singleNcores, aggregatedCube, format, processing, outputFolder, user, password, hostname, port, mode, logFlag)
	else:
		print("Aggregation not recognized")

	if logFlag == True:
		end_time = timeit.default_timer() - start_time_begin
		logging.debug('[%s] [%s - %s] #%s# execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), processing, str(end_time))

	#Remove tmp cubes
	#cube.Cube.client.submit("oph_delete cube=[container=bigsea;level!=0;cube_filter!="+str(aggregatedCube.pid.split("/")[-1])+"]")

	return outFile

def removeTempCubes(singleNcores, user, password, hostname, port, logFlag):

	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time_begin = timeit.default_timer()

	#Initialize
	sys.stdout = open(os.devnull, 'w')
	if user is "__TOKEN__":
		cube.Cube.setclient(token=password, server=hostname, port=port)
	else:
		cube.Cube.setclient(username=user, password=password, server=hostname, port=port)
	sys.stdout = sys.__stdout__;

	#Remove tmp cubes
	cube.Cube.client.submit("oph_delete cube=[container=bigsea;level=1|2|3|4|5|6|7|8|9|10|11|12]")
	#cube.Cube.deletecontainer(container=str(sample_container), delete_type='physical', hidden='no')

	if logFlag == True:
		end_time = timeit.default_timer() - start_time_begin
		logging.debug('[%s] [%s - %s] Delete execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	return True

