import os, json, pandas, numpy, calendar, datetime
import multiprocessing

import sys
from PyOphidia import cube, client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common

from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on


def extractPhase(inputFiles, tmpFolder, procType):

	#Loop on input files
	data = [0 for m in range(0, len(inputFiles))]
	for i, e in enumerate(inputFiles):
		data[i] = extractFromFile(tmpFolder, e)
	data = compss_wait_on(data)

	data = pandas.concat([d for d in data], ignore_index=True)

	outputData = []
	if procType == "busUsage":
		data.sort_values(['CODLINHA', 'NOMELINHA', 'CODVEICULO', 'DATAUTILIZACAO'], ascending=[True, True, True, True], inplace=True)

		line = data.values[:,0].flatten('F')
		vehicle = data.values[:,1].flatten('F')
		time = data.values[:,2].flatten('F')
		outputData = [line, vehicle, time]

	elif procType == "passengerUsage":
		data.sort_values(['NUMEROCARTAO', 'CODLINHA', 'NOMELINHA', 'CODVEICULO', 'DATAUTILIZACAO'], ascending=[True, True, True, True, True], inplace=True)

		line = data.values[:,0].flatten('F')
		time = data.values[:,2].flatten('F')
		number = data.values[:,4].flatten('F')
		outputData = [number, line, time]

	else:
		raise RuntimeError("Type of processing not recognized")

	return outputData

@task(inputFolder=IN, inputName=IN, returns=pandas.DataFrame)
def extractFromFile(inputFolder, inputName):

	inputFile = os.path.join(inputFolder, inputName)
	if os.path.isfile(inputFile):
		inFilename, inFileExt = os.path.splitext(inputFile)
		if inFileExt == '.json':
			print("Extract from \"" + inputName + "\"")
			#Parse text to remove all empty lines
			with open(inputFile, 'r') as f:
				json_list = []
				for line in f:
					if line.strip():
						json_list.append(str(line))

				json_text = "".join(json_list)

			#Convert from json to Pandas dataframe
			newData = pandas.read_json(json_text, lines=False)

			return newData

	return None

def transformToNetCDF(data, outputFolder, multiProcesses, procType):

	if procType == "busUsage":
		time_period = 3600
		x = data[0]
		y = data[1]
		t = data[2]

		diff_y = [y[i] != y[i+1] for i in range(0,len(y)-1)]
		diff_x = [x[i] != x[i+1] for i in range(0,len(x)-1)]
		diff = numpy.logical_or(diff_x, diff_y)  

		t = pandas.to_datetime(t, format='%d/%m/%y %H:%M:%S,%f')

		#Split time array based on external dimensions
		sub_times = numpy.split(t, numpy.where(diff)[0]+1)
		sub_x = [numpy.unique(sx) for sx in numpy.split(x, numpy.where(diff)[0]+1)]
		sub_y = [numpy.unique(sy) for sy in numpy.split(y, numpy.where(diff)[0]+1)]

		x = numpy.unique(x)
		y = numpy.unique(y)

		#Define time dimension (aggregate on time period)
		start_date = min(t)
		end_date = max(t)
		interval = end_date.date() - start_date.date()
		start_time = calendar.timegm(start_date.date().timetuple())
		time_len = (interval.days + 1)*int((24*3600)/time_period)
		#Time val contains also 24 steps for final day
		time_val = [start_time + i*time_period for i in range(0,time_len+1)]

		measure = numpy.full([len(x),len(y),time_len],numpy.nan, dtype=numpy.float32)

		#Aggregate times
		#pool = multiprocessing.Pool(processes=multiProcesses)
		#results = pool.map(common.aggregateData, [(ar, time_val) for idx, ar in enumerate(sub_times)])
		results = []
		for idx, ar in enumerate(sub_times):
			results.append(common.aggregateData((ar, time_val)))

		for idx, ar in enumerate(sub_times):
			x_index = (numpy.where(x==sub_x[idx])[0])
			y_index = (numpy.where(y==sub_y[idx])[0])
			measure[x_index, y_index, :] = results[idx]

		#Create NetCDF file
		start_time = datetime.datetime.strptime(datetime.datetime.utcfromtimestamp(time_val[0]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
		times = [start_time + datetime.timedelta(hours=0.5) + n *datetime.timedelta(hours=1) for n in range(time_len)]
		outputFile = os.path.join(outputFolder, "traffic_" + str(datetime.date.today()) + ".nc")
		common.createNetCDFFileBusUsage(outputFile, x, y, times, measure)

	elif procType == "passengerUsage":
		time_period = 86400
		x = data[0]
		y = data[1]
		t = data[2]

		diff_y = [y[i] != y[i+1] for i in range(0,len(y)-1)]
		diff_x = [x[i] != x[i+1] for i in range(0,len(x)-1)]
		diff = numpy.logical_or(diff_x, diff_y)  

		t = pandas.to_datetime(t, format='%d/%m/%y %H:%M:%S,%f')

		#Split time array based on external dimensions
		sub_times = numpy.split(t, numpy.where(diff)[0]+1)
		sub_x = [numpy.unique(sx) for sx in numpy.split(x, numpy.where(diff)[0]+1)]
		sub_y = [numpy.unique(sy) for sy in numpy.split(y, numpy.where(diff)[0]+1)]

		x = numpy.unique(x)
		y = numpy.unique(y)

		#Define time dimension (aggregate on time period)
		start_date = min(t)
		end_date = max(t)
		interval = end_date.date() - start_date.date()
		start_time = calendar.timegm(start_date.date().timetuple())
		time_len = (interval.days + 1)*int((24*3600)/time_period)
		#Time val contains also 24 steps for final day
		time_val = [start_time + i*time_period for i in range(0,time_len+1)]

		measure = numpy.full([len(x),len(y),time_len],numpy.nan, dtype=numpy.float32)

		#Aggregate times
		#pool = multiprocessing.Pool(processes=multiProcesses)
		#results = pool.map(common.aggregateData, [(ar, time_val) for idx, ar in enumerate(sub_times)])
		results = []
		for idx, ar in enumerate(sub_times):
			results.append(common.aggregateData((ar, time_val)))

		for idx, ar in enumerate(sub_times):
			x_index = (numpy.where(x==sub_x[idx])[0])
			y_index = (numpy.where(y==sub_y[idx])[0])
			measure[x_index, y_index, :] = results[idx]

		#Replace hash value with its position
		x = [p for p,v in enumerate(x)]

		#Create NetCDF file
		start_time = datetime.datetime.strptime(datetime.datetime.utcfromtimestamp(time_val[0]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
		times = [start_time + datetime.timedelta(days=0.5) + n *datetime.timedelta(days=1) for n in range(time_len)]
		outputFile = os.path.join(outputFolder, "traffic_" + str(datetime.date.today()) + ".nc")

		common.createNetCDFFilePassengerUsage(outputFile, x, y, times, measure)
	else:
		raise RuntimeError("Type of processing not recognized")

	return times, outputFile

def loadOphidia(inputFile, times, singleNcores, user, password, hostname, port, procType):

	if procType == "busUsage":
		measure = "passengers"
		imp_concept_level = "h"
	elif procType == "passengerUsage":
		measure = "usage"
		imp_concept_level = "d"
	else:
		raise RuntimeError("Type of processing not recognized")

	sys.stdout = open(os.devnull, 'w')

	cube.Cube.setclient(user, password, hostname, port)

	try:
		cube.Cube.createcontainer(container='bigsea',dim='cod_passenger|cod_linha|cod_veiculo|time',dim_type='long|long|long|double',hierarchy='oph_base|oph_base|oph_base|oph_time',display=False,base_time='2015-01-01 00:00:00',calendar='gregorian',units='h')
	except:
		pass

	historicalCube = cube.Cube.importnc(container='bigsea', measure=measure, imp_dim='time', imp_concept_level=imp_concept_level, import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=inputFile , display=False,ncores=singleNcores)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='datacube_name',metadata_value='historical', display=False)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='start_date',metadata_value=str(times[0].date()), display=False)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='end_date',metadata_value=str(times[-1].date()), display=False)

	sys.stdout = sys.__stdout__; 

