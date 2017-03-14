import sys, time, json, numpy, calendar, datetime, os
import multiprocessing

from PyOphidia import cube, client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common

METRICS = ['MIN', 'MAX', 'AVG', 'SUM']

#Functions for Ophidia aggregations
def simpleAggregation(historicalCube, metric, parallelNcores):
	return historicalCube.aggregate(group_size='all',operation=metric,ncores=parallelNcores)

def reducedAggregation(historicalCube, metric, spatialReduction, parallelNcores):
	reducedCube = historicalCube.reduce2(dim='time',concept_level=spatialReduction,operation=metric,ncores=parallelNcores)
	return reducedCube.aggregate(group_size='all',operation=metric,ncores=parallelNcores)

def totalAggregation(subsettedCube, metric, parallelNcores):
	reducedCube = subsettedCube.reduce(group_size='all',operation=metric,ncores=parallelNcores)
	return reducedCube.aggregate(group_size='all',operation=metric,ncores=parallelNcores)

def totalHourlyAggregation(subsettedCube, metric, parallelNcores):
	reducedCube = subsettedCube.apply(query="oph_concat(oph_reduce(oph_get_subarray2(measure,'1:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'2:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'3:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'4:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'5:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'6:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'7:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'8:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'9:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'10:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'11:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'12:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'13:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'14:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'15:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'16:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'17:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'18:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'19:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'20:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'21:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'22:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'23:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'24:24:end'),'OPH_"+metric+"'))", check_type='no', measure_type='auto',ncores=parallelNcores)
	return reducedCube.aggregate(group_size='all',operation=metric,ncores=parallelNcores)

def fullHourlyAggregation(aggregatedCube, metric, parallelNcores):
	reducedCube = aggregatedCube.apply(query="oph_concat(oph_reduce(oph_get_subarray2(measure,'1:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'2:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'3:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'4:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'5:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'6:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'7:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'8:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'9:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'10:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'11:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'12:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'13:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'14:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'15:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'16:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'17:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'18:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'19:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'20:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'21:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'22:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'23:24:end'),'OPH_"+metric+"'),oph_reduce(oph_get_subarray2(measure,'24:24:end'),'OPH_"+metric+"'))", check_type='no', measure_type='auto',ncores=parallelNcores)
	mergedCube = reducedCube.merge(nmerge=0,ncores=1)
	return mergedCube.aggregate(group_size='all',operation=metric,ncores=1)

def aggregationFuncWrapper(args):
	functionName = args[0]
	args = list(args)
	del args[0]
	args = tuple(args)

	if functionName == "fullHourlyAggregation":
		return fullHourlyAggregation(*args)
	elif functionName == "totalHourlyAggregation":
		return totalHourlyAggregation(*args)
	elif functionName == "totalAggregation":
		return totalAggregation(*args)
	elif functionName == "reducedAggregation":
		return reducedAggregation(*args)
	elif functionName == "simpleAggregation":
		return simpleAggregation(*args)

def buildValues(aggregation, cubeList, day):

	dataList = []
	for c in cubeList:
		#Extract data
		data = c.export_array(show_time='yes')
		dataList.append(data)

	#Get dimension and measure values
	codLinhaData = None
	dateData = None
	passengerData = []
	tmpDat = dataList[0]

	if not tmpDat:
		exit("ERROR: Missing datacube")

	for k in tmpDat['dimension']:
		if aggregation == 'weekdays-peakhours' or aggregation == 'weekdaysets-peakhours':
			codLinhaData = ""
		else:
			if(k['name'] == 'cod_linha'):
				codLinhaData = k['values']

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

	for d in dataList:
		for k in d['measure']:
			if(k['name'] == 'passengers'):
				passengerData.append(k['values'])

	if codLinhaData == None or dateData == None or not passengerData:
		exit("ERROR: Variables not found")

	return codLinhaData, dateData, passengerData


def basicComputation(parallelNcores, singleNcores, historicalCube, format, aggregation, outputFolder):
	cubeList = []
	if aggregation == 'weekly-lines':
		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("reducedAggregation", historicalCube, m.lower(), 'w', parallelNcores) for m in METRICS])
	elif aggregation == 'monthly-lines':
		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("reducedAggregation", historicalCube, m.lower(), 'M', parallelNcores) for m in METRICS])
	elif aggregation == 'daily-lines':
		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("reducedAggregation", historicalCube, m.lower(), 'd', parallelNcores) for m in METRICS])
	elif aggregation == 'hourly-lines':
		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("simpleAggregation", historicalCube, m.lower(), parallelNcores) for m in METRICS])

	#Get dimension and measure values
	codLinhaData = None
	dateData = None
	passengerData = []
	codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList, None)

	#Build json file and array for plot
	if format == 'json':
		outFile = common.createJSONFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
	else:
		outFile = common.createCSVFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)

	return outFile

def weekdayLinesAggregation(parallelNcores, singleNcores, aggregation, historicalCube, startDate, numDays, format, outputFolder):
	#weekdays array
	if aggregation == 'weekdays-hourly-lines': 
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	for i, day in enumerate(weekDays):
		#Build filter set 
		filter_list = ""
		if aggregation == 'weekdays-hourly-lines': 
			filter_list = common.buildSubsetFilter(startDate, numDays, i+1) 
		else:
			for j in weekDaysId[i]:
				filter_list = filter_list + common.buildSubsetFilter(startDate, numDays, j) + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			exit("ERROR: Subset filter creation")

		subsettedCube = historicalCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='yes',ncores=singleNcores)  

		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("totalHourlyAggregation", subsettedCube, m.lower(), parallelNcores) for m in METRICS])

		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList, day)

		#Build json file and array for plot
		if i == 0:
			if format == 'json':
				outFile = common.createJSONFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
			else:
				outFile = common.createCSVFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
		else:
			if format == 'json':
				outFile = common.createJSONFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'a', 0)
			else:
				outFile = common.createCSVFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'a', 0)

	return outFile

def weekdayLinesTotalAggregation(parallelNcores, singleNcores, aggregation, historicalCube, startDate, numDays, format, outputFolder):
	#weekdays array
	if aggregation == 'weekdays-lines': 
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	for i, day in enumerate(weekDays):
		#Build filter set 
		filter_list = ""
		if aggregation == 'weekdays-lines': 
			filter_list = common.buildSubsetFilter(startDate, numDays, i+1) 

		else:
			for j in weekDaysId[i]:
				filter_list = filter_list + common.buildSubsetFilter(startDate, numDays, j) + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			exit("ERROR: Subset filter creation")

		subsettedCube = historicalCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='yes',ncores=singleNcores)  

		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("totalAggregation", subsettedCube, m.lower(), parallelNcores) for m in METRICS])

		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList, day)

		#Build json file and array for plot
		if i == 0:
			if format == 'json':
				outFile = common.createJSONFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
			else:
				outFile = common.createCSVFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'w', 0)
		else:
			if format == 'json':
				outFile = common.createJSONFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'a', 0)
			else:
				outFile = common.createCSVFile(outputFolder, aggregation, passengerData, codLinhaData, dateData, 'a', 0)

	return outFile

def peakhourAggregation(parallelNcores, singleNcores, aggregation, historicalCube, startDate, numDays, format, outputFolder):
	#weekdays array
	if aggregation == 'weekdays-peakhours': 		
		weekDays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
	else:
		weekDays = ["Saturday/Sunday","Monday/Friday","Tuesday/Wednesday/Thursday"]
		weekDaysId = [[6,7],[1,5],[2,3,4]]

	for i, day in enumerate(weekDays):
		#Build filter set 
		filter_list = ""
		if aggregation == 'weekdays-peakhours': 		
			filter_list = common.buildSubsetFilter(startDate, numDays, i+1) 
		else:
			for j in weekDaysId[i]:
				filter_list = filter_list + common.buildSubsetFilter(startDate, numDays, j) + ","

			filter_list = filter_list[:-1]

		if not filter_list:
			exit("ERROR: Subset filter creation")

		subsettedCube = historicalCube.subset2(subset_dims='time',subset_filter=filter_list,time_filter='yes',ncores=singleNcores)  
		aggregatedCube = subsettedCube.aggregate(group_size='all',operation='sum',ncores=singleNcores)

		pool = multiprocessing.Pool(processes=4)
		cubeList = pool.map(aggregationFuncWrapper, [("fullHourlyAggregation", aggregatedCube, m.lower(), parallelNcores) for m in METRICS])

		#Get dimension and measure values
		codLinhaData = None
		dateData = None
		passengerData = []
		codLinhaData, dateData, passengerData = buildValues(aggregation, cubeList, day)

		#Build json file and array for plot
		if i == 0:
			if format == 'json':
				outFile = common.createJSONFile(outputFolder, aggregation, passengerData, None, dateData, 'w', 1)
			else:
				outFile = common.createCSVFile(outputFolder, aggregation, passengerData, None, dateData, 'w', 1)
		else:
			if format == 'json':
				outFile = common.createJSONFile(outputFolder, aggregation, passengerData, None, dateData, 'a', 1)
			else:
				outFile = common.createCSVFile(outputFolder, aggregation, passengerData, None, dateData, 'a', 1)

	return outFile


def computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, processing, format, outputFolder):

	#Initialize
	sys.stdout = open(os.devnull, 'w')
	cube.Cube.setclient(user, password, hostname, port)	
	sys.stdout = sys.__stdout__; 

	#Get Historical cube PID from metadata
	cube.Cube.search(container_filter='bigsea',metadata_key_filter='datacube_name',metadata_value_filter='historical',display=False)
	data = json.loads(cube.Cube.client.last_response)
	if not data['response'][0]['objcontent'][0]['rowvalues']:
		exit("ERROR: Historical datacube not found")
	else:
		historicalCubePid = data['response'][0]['objcontent'][0]['rowvalues'][0][0]

	#Get Historical start/end date from metadata
	cube.Cube.search(container_filter='bigsea',metadata_key_filter='start_date',display=False)
	data = json.loads(cube.Cube.client.last_response)
	if not data['response'][0]['objcontent'][0]['rowvalues']:
		exit("ERROR: Historical datacube not found")
	else:
		startDate = data['response'][0]['objcontent'][0]['rowvalues'][0][2]
		startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")

	cube.Cube.search(container_filter='bigsea',metadata_key_filter='end_date',display=False)
	data = json.loads(cube.Cube.client.last_response)
	if not data['response'][0]['objcontent'][0]['rowvalues']:
		exit("ERROR: Historical datacube not found")
	else:
		endDate = data['response'][0]['objcontent'][0]['rowvalues'][0][2]
		endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

	historicalCube = cube.Cube(pid=historicalCubePid)

	#Subset on weekdays (monday is 0)
	numDays = (endDate - startDate).days + 1
	startDay = startDate.isoweekday()

	if processing == 'hourly-lines' or processing == 'daily-lines' or processing == 'weekly-lines' or processing == 'monthly-lines':
		outFile = basicComputation(parallelNcores, singleNcores, historicalCube, format, processing, outputFolder)
	elif processing == 'weekdays-hourly-lines' or processing == 'weekdaysets-hourly-lines':
		outFile = weekdayLinesAggregation(parallelNcores, singleNcores, processing, historicalCube, startDate, numDays, format, outputFolder)
	elif processing == 'weekdays-lines' or processing == 'weekdaysets-lines':
		outFile = weekdayLinesTotalAggregation(parallelNcores, singleNcores, processing, historicalCube, startDate, numDays, format, outputFolder)
	elif processing == 'weekdays-peakhours' or processing == 'weekdaysets-peakhours':
		outFile = peakhourAggregation(parallelNcores, singleNcores, processing, historicalCube, startDate, numDays, format, outputFolder)
	else:
		print("Aggregation not recognized")
		
	#Remove tmp cubes
	cube.Cube.client.submit("oph_delete cube=[container=bigsea;level=1|2|3|4|5|6]")
	#cube.Cube.deletecontainer(container=str(sample_container), delete_type='physical', hidden='no')

	return outFile
