import sys, os, shutil, subprocess, json, pandas, numpy
from PyOphidia import cube, client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common

import timeit
import logging
import inspect
from datetime import datetime

def internalAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile):

	inputFile = os.path.join(inputFolder, inputName)
	if os.path.isfile(inputFile):
		inFilename, inFileExt = os.path.splitext(inputFile)
		if inFileExt == '.txt':
			print("Anonymizing file (Phase 1): \"" + inputName + "\"")
			newFile = common.jsonLine2json(inputFile)

			try:
				proc = subprocess.Popen(["java -jar " + anonymizationBin + " " + newFile + " " + policyFile], cwd=os.path.dirname(os.path.abspath(__file__)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
			except OSError:
				print("Unable to run anonymization tool")

			command_resp, command_error = proc.communicate()
			if command_error:
				print("Anonymization error:" + command_error)

			command_resp = command_resp.decode("utf-8")

			if 'Anonymized document generated in: ' in command_resp:
				anonymFile = command_resp.split("Anonymized document generated in: ",1)[1] 
			else:
				raise RuntimeError("Error in running anoymization")

			os.remove(newFile)
			path, name = os.path.split(anonymFile)
			outName = os.path.join(tmpFolder, name)
			shutil.move(anonymFile, outName)

			return outName

	return None

def internalExtractFromFile(inputFolder, inputName):

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

			os.remove(inputFile)
			return newData

	return None

def internalTransform(sub_x, sub_y, sub_times, x, y, time_val):

	#We assume the function will work on a subset of continuos rows
	from bisect import bisect_left
	first_x_index = (bisect_left(x, sub_x[0]))
	last_x_index = (bisect_left(x, sub_x[-1]))

	x_len = last_x_index - first_x_index + 1

	measure = numpy.full([x_len,len(y),len(time_val)-1],numpy.nan, dtype=numpy.float32)
	for idx, ar in enumerate(sub_times):
		x_index = (bisect_left(x, sub_x[idx]))
		y_index = (bisect_left(y, sub_y[idx]))
		measure[(x_index-first_x_index), y_index, :] = common.aggregateData((ar, time_val))

	return measure


#Functions for Ophidia aggregations
def internalSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port, logFlag=False):
	cube.Cube.setclient(username=user, password=pwd, server=host, port=port)
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	aggregatedCube = startCube.aggregate(group_size='all',operation=metric,ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] AGGREGATE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	data = aggregatedCube.export_array(show_time='yes')
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] EXPLORE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
	return data

def internalReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, logFlag=False):
	cube.Cube.setclient(username=user, password=pwd, server=host, port=port)
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	reducedCube = startCube.reduce2(dim='time',concept_level=spatialReduction,operation=metric,ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] REDUCE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	data = reducedCube.export_array(show_time='yes')
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] EXPLORE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
	return data

def internalVerticalAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port, logFlag=False):
	cube.Cube.setclient(username=user, password=pwd, server=host, port=port)
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	aggregatedCube = startCube.aggregate2(dim='time',concept_level=spatialReduction,operation=metric,ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] AGGREGATE2 execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	data = aggregatedCube.export_array(show_time='yes')
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] EXPLORE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
	for k in data['measure']:
		if(k['name'] == 'usage'):
			k['values'] = map(list, zip(*k['values']))

	return data

def internalTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port, logFlag=False):
	cube.Cube.setclient(username=user, password=pwd, server=host, port=port)
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	reducedCube = startCube.reduce(group_size='all',operation=metric,ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] REDUCE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	data = reducedCube.export_array(show_time='yes')
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] EXPLORE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
	return data

def internalTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port, logFlag=False):
	cube.Cube.setclient(username=user, password=pwd, server=host, port=port)
	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()
	reducedCube1 = startCube.apply(query="oph_concat('OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT','OPH_FLOAT',oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'1:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'3:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'5:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'7:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'9:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'11:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'13:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'15:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'17:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'19:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'21:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'23:24:end'),'OPH_"+metric+"'))", check_type='no', measure_type='manual',ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] REDUCE PART1 execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	reducedCube2 = startCube.apply(query="oph_concat('OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT|OPH_FLOAT','OPH_FLOAT',oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'2:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'4:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'6:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'8:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'10:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'12:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'14:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'16:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'18:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'20:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'22:24:end'),'OPH_"+metric+"'),oph_reduce('OPH_FLOAT','OPH_FLOAT',oph_get_subarray2('OPH_FLOAT','OPH_FLOAT',measure,'24:24:end'),'OPH_"+metric+"'))", check_type='no', measure_type='manual',ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] REDUCE PART2 execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	mergedCube = cube.Cube.mergecubes(cubes=reducedCube1.pid+'|'+reducedCube2.pid, ncores=parallelNcores)
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] MERGECUBES execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
		start_time = timeit.default_timer()
	data = mergedCube.export_array(show_time='yes')
	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] EXPLORE execution time: %s [s]', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
	return data

