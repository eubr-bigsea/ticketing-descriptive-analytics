import os, json, pandas, numpy, calendar, datetime
import multiprocessing

import sys
from PyOphidia import cube, client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common


def aggregatePassengers(args):
	ar = args[0]
	time_val = args[1]
	measure = numpy.full([len(time_val)-1],numpy.nan, dtype=numpy.float32)
	time = [calendar.timegm(k.timetuple()) for k in ar]
	for time_index in range(0,len(time_val)-1):
		for t in time:
			if t >= time_val[time_index] and t < time_val[time_index+1]: 
				if not numpy.isnan(measure[time_index]):
					measure[time_index] += 1
				else:
					measure[time_index] = 1

	return measure


def extractFromFiles(inputFolder):

	#Loop on input files
	data = None
	for e in sorted(os.listdir(inputFolder)):
		inputFile = os.path.join(inputFolder, e)
		if os.path.isfile(inputFile):
			inFilename, inFileExt = os.path.splitext(inputFile)
			if inFileExt == '.json':
				print("Extract from \"" + e + "\"")
				#Parse text to remove all empty lines
				with open(inputFile, 'r') as f:
					json_list = []
					for line in f:
						if line.strip():
							json_list.append(str(line))

					json_text = "".join(json_list)

				#Convert from json to Pandas dataframe
				newData = pandas.read_json(json_text, lines=False)
				data = pandas.concat([data,newData])

	data.sort_values(['CODLINHA', 'NOMELINHA', 'CODVEICULO', 'DATAUTILIZACAO'], ascending=[True, True, True, True], inplace=True)

	line = data.values[:,0].flatten('F')
	vehicle = data.values[:,1].flatten('F')
	date = data.values[:,2].flatten('F')

	return line, vehicle, date


def transformToNetCDF(x, y, t, outputFolder, multiProcesses):

	time_period = 3600

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
	pool = multiprocessing.Pool(processes=multiProcesses)
	results = pool.map(aggregatePassengers, [(ar, time_val) for idx, ar in enumerate(sub_times)])

	for idx, ar in enumerate(sub_times):
		x_index = (numpy.where(x==sub_x[idx])[0])
		y_index = (numpy.where(y==sub_y[idx])[0])
		measure[x_index, y_index, :] = results[idx]

	#Create NetCDF file
	start_time = datetime.datetime.strptime(datetime.datetime.utcfromtimestamp(time_val[0]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
	times = [start_time + datetime.timedelta(hours=0.5) + n *datetime.timedelta(hours=1) for n in range(time_len)]
	outputFile = os.path.join(outputFolder, "traffic_" + str(datetime.date.today()) + ".nc")
	common.createNetCDFFile(outputFile, x, y, times, measure)

	return times, outputFile


def loadOphidia(inputFile, times, singleNcores, user, password, hostname, port):

	sys.stdout = open(os.devnull, 'w')

	cube.Cube.setclient(user, password, hostname, port)

	try:
		cube.Cube.createcontainer(container='bigsea',dim='cod_linha|cod_veiculo|time',dim_type='long|long|double',hierarchy='oph_base|oph_base|oph_time',display=False,base_time='2015-01-01 00:00:00',calendar='gregorian',units='h')
	except:
		pass

	historicalCube = cube.Cube.importnc3(container='bigsea', measure='passengers', imp_dim='time', imp_concept_level='h', import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=inputFile , display=False,ncores=singleNcores)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='datacube_name',metadata_value='historical', display=False)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='start_date',metadata_value=str(times[0].date()), display=False)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='end_date',metadata_value=str(times[-1].date()), display=False)

	sys.stdout = sys.__stdout__; 

