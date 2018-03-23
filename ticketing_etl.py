import os, json, pandas, numpy, calendar, datetime, time, math
import multiprocessing

import sys
from PyOphidia import cube, client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common

def extractPhase(inputFiles, tmpFolder, procType, dq_flag, mode, delFlag):

	#Loop on input files
	data = [0 for m in range(0, len(inputFiles))]
	for i, e in enumerate(inputFiles):
		data[i] = extractFromFile(tmpFolder, e, procType, mode, delFlag)

	if mode == "compss":
		from pycompss.api.api import compss_wait_on
		data = compss_wait_on(data)

	data = pandas.concat([d for d in data], ignore_index=True)

	outputData = []
	if procType == "busUsage":
		data.sort_values(['CODLINHA', 'CODVEICULO', 'DATAUTILIZACAO'], ascending=[True, True, True], inplace=True)

		line = data['CODLINHA'].values.flatten('F')
		vehicle = data['CODVEICULO'].values.flatten('F')
		time = data['DATAUTILIZACAO'].values.flatten('F')

		completeness = None
		consistency = None
		timeliness = None
		if dq_flag == True and 'COMPLETENESS_MISSING' in data and "TIMELINESS_DATAUTILIZACAO" in data and "ASSOCIATION_CONSISTENCY" in data:
			completeness = data['COMPLETENESS_MISSING'].values.flatten('F')
			consistency = data['ASSOCIATION_CONSISTENCY'].values.flatten('F')
			timeliness = data['TIMELINESS_DATAUTILIZACAO'].values.flatten('F')

		outputData = [line, vehicle, time, completeness, consistency, timeliness]

	elif procType == "passengerUsage":
		data.sort_values(['NUMEROCARTAO', 'CODLINHA', 'CODVEICULO', 'DATAUTILIZACAO'], ascending=[True, True, True, True], inplace=True)

		line = data['CODLINHA'].values.flatten('F')
		time = data['DATAUTILIZACAO'].values.flatten('F')
		number = data['NUMEROCARTAO'].values.flatten('F')
		birthDate = data['DATANASCIMENTO'].values.flatten('F')
		gender = data['SEXO'].values.flatten('F')

		completeness = None
		consistency = None
		timeliness = None
		if dq_flag == True and 'COMPLETENESS_MISSING' in data and "TIMELINESS_DATAUTILIZACAO" in data and "ASSOCIATION_CONSISTENCY" in data:
			completeness = data['COMPLETENESS_MISSING'].values.flatten('F')
			consistency = data['ASSOCIATION_CONSISTENCY'].values.flatten('F')
			timeliness = data['TIMELINESS_DATAUTILIZACAO'].values.flatten('F')

		outputData = [number, line, time, birthDate, gender, completeness, consistency, timeliness]

	elif procType == "busStops":
		data.sort_values(['stopPointId', 'route', 'timestamp'], ascending=[True, True, True], inplace=True)

		busStop = data['stopPointId'].values.flatten('F')
		line = data['route'].values.flatten('F')
		time = data['timestamp'].values.flatten('F')
		passengers = data['numberTickets'].values.flatten('F')

		outputData = [busStop, line, time, passengers]
	else:
		raise RuntimeError("Type of processing not recognized")

	return outputData

def extractFromFile(inputFolder, inputName, procType, mode, delFlag):
	if procType != "busStops":
		if mode == 'compss':
			from compss_functions import compssExtractFromFile
			return compssExtractFromFile(inputFolder, inputName, delFlag)
		else:
			from internal_functions import internalExtractFromFile
			return internalExtractFromFile(inputFolder, inputName, delFlag)
	else:
		#Pre-process CSV file from EMaaS
		if mode == 'compss':
			from compss_functions import compssExtractFromEMFile
			return compssExtractFromEMFile(inputFolder, inputName)
		else:
			from internal_functions import internalExtractFromEMFile
			return internalExtractFromEMFile(inputFolder, inputName)

def transformToNetCDF(data, outputFolder, multiProcesses, procType, mode):

	if procType == "busUsage":
		time_period = 3600
		x = data[0]
		y = data[1]
		t = data[2]
		dq1 = data[3]
		dq2 = data[4]
		dq3 = data[5]

		diff_y = [y[i] != y[i+1] for i in range(0,len(y)-1)]
		diff_x = [x[i] != x[i+1] for i in range(0,len(x)-1)]
		diff = numpy.logical_or(diff_x, diff_y)

		t = pandas.to_datetime(t, format='%d/%m/%y %H:%M:%S,%f')

		#Split time array based on external dimensions
		records_split = numpy.where(diff)[0]+1
		sub_times = numpy.split(t, records_split)
		if dq1 is not None and dq2 is not None and dq3 is not None:
			sub_dq1 = numpy.split(dq1, records_split)
			sub_dq2 = numpy.split(dq2, records_split)
			sub_dq3 = numpy.split(dq3, records_split)
		#Add index for first element
		records_split = numpy.insert(records_split,0,0)
		sub_x = numpy.take(x, records_split)
		sub_y = numpy.take(y, records_split)

		x = numpy.unique(x)
		y = numpy.unique(y)

		#Define partitions for concurrent execution
		if int(multiProcesses) > 1:
			#Compute exact number or records per task
			minNum = math.floor(len(t) / float(multiProcesses))
			remainder = len(t) % minNum
			record_task = []
			for i in range(0,int(multiProcesses)):
				if remainder > 0:
					record_task.append(minNum +1)
					remainder = remainder - 1
				else:
					record_task.append(minNum)

			#Compute number of matrix rows per task
			row_task = []
			count = 0
			for i in diff_x:
				count = count + 1
				if i == True:
					row_task.append(count)
					count = 0
			#Append last
			count = count + 1
			row_task.append(count)

			#Compute number of sub_times per task based on row
			partitions = []
			count = 0
			for i in range(0,int(multiProcesses)):
				while row_task:
					count = count + row_task.pop(0)
					if count >= record_task[i] or not row_task:
						partitions.append(count)
						count = 0
						break

			#Split arrays
			count = 0
			current = 0
			splits = []
			for p in partitions:
				for i in range(current,len(sub_times)):
					count = count + len(sub_times[i])
					current = current + 1
					if count == p or not sub_times:
						splits.append(current)
						count = 0
						break

			sub_times = numpy.array_split(sub_times, splits)
			sub_x = numpy.array_split(sub_x, splits)
			sub_y = numpy.array_split(sub_y, splits)
			if dq1 is not None and dq2 is not None and dq3 is not None:
				sub_dq1 = numpy.array_split(sub_dq1, splits)
				sub_dq2 = numpy.array_split(sub_dq2, splits)
				sub_dq3 = numpy.array_split(sub_dq3, splits)

			threadNum = len(splits) if len(splits) < multiProcesses else multiProcesses
		else:
			sub_times = [sub_times]
			sub_x = [sub_x]
			sub_y = [sub_y]
			if dq1 is not None and dq2 is not None and dq3 is not None:
				sub_dq1 = [sub_dq1]
				sub_dq2 = [sub_dq2]
				sub_dq3 = [sub_dq3]
			threadNum = 1

		#Define time dimension (aggregate on time period)
		start_date = min(t)
		end_date = max(t)
		interval = end_date.date() - start_date.date()
		start_time = calendar.timegm(start_date.date().timetuple())
		time_len = (interval.days + 1)*int((24*3600)/time_period)
		#Time val contains also 24 steps for final day
		time_val = [start_time + i*time_period for i in range(0,time_len+1)]

		results = [0 for i in range(0, int(threadNum))]
		if dq1 is not None and dq2 is not None and dq3 is not None:
			results_dq1 = [0 for i in range(0, int(threadNum))]
			results_dq2 = [0 for i in range(0, int(threadNum))]
			results_dq3 = [0 for i in range(0, int(threadNum))]

		for i in range(0, int(threadNum)):
			if mode == 'compss':
				if dq1 is not None and dq2 is not None and dq3 is not None:
					from compss_functions import compssTransformDQ
					results[i] = compssTransformDQ(sub_x[i], sub_y[i], sub_times[i], sub_dq1[i], sub_dq2[i], sub_dq3[i], x, y, time_val)
				else:
					from compss_functions import compssTransform
					results[i] = compssTransform(sub_x[i], sub_y[i], sub_times[i], x, y, time_val)
			else:
				if dq1 is not None and dq2 is not None and dq3 is not None:
					from internal_functions import internalTransformDQ
					res = internalTransformDQ(sub_x[i], sub_y[i], sub_times[i], sub_dq1[i], sub_dq2[i], sub_dq3[i], x, y, time_val)
					results[i] = res[0]
					results_dq1[i] = res[1]
					results_dq2[i] = res[2]
					results_dq3[i] = res[3]
				else:
					from internal_functions import internalTransform
					results[i] = internalTransform(sub_x[i], sub_y[i], sub_times[i], x, y, time_val)

		if mode == 'compss':
			from pycompss.api.api import compss_wait_on
			results = compss_wait_on(results)
			if dq1 is not None and dq2 is not None and dq3 is not None:
				res = results[:]
				for i in range(len(res)):
					results[i] = res[i][0]
					results_dq1[i] = res[i][1]
					results_dq2[i] = res[i][2]
					results_dq3[i] = res[i][3]

		resultList = []
		for r in results:
			resultList.append(r)

		measure = numpy.concatenate(resultList, axis=0)

		if dq1 is not None and dq2 is not None and dq3 is not None:
			resultList = []
			for r in results_dq1:
				resultList.append(r)
			measure_dq1 = numpy.concatenate(resultList, axis=0)
			resultList = []
			for r in results_dq2:
				resultList.append(r)
			measure_dq2 = numpy.concatenate(resultList, axis=0)
			resultList = []
			for r in results_dq3:
				resultList.append(r)
			measure_dq3 = numpy.concatenate(resultList, axis=0)

		#Create NetCDF file
		start_time = datetime.datetime.strptime(datetime.datetime.utcfromtimestamp(time_val[0]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
		times = [start_time + datetime.timedelta(hours=0.5) + n *datetime.timedelta(hours=1) for n in range(time_len)]
		outputFile = []
		outputFile.append(os.path.join(outputFolder, "traffic_" + str(time.time()) + ".nc"))
		common.createNetCDFFileBusUsage(outputFile[0], x, y, times, measure,'passengers')

		if dq1 is not None and dq2 is not None and dq3 is not None:
			outputFile.append(os.path.join(outputFolder, "traffic_dq1_" + str(time.time()) + ".nc"))
			common.createNetCDFFileBusUsage(outputFile[1], x, y, times, measure_dq1,'passengers_completeness')
			outputFile.append(os.path.join(outputFolder, "traffic_dq2_" + str(time.time()) + ".nc"))
			common.createNetCDFFileBusUsage(outputFile[2], x, y, times, measure_dq2,'passengers_consistency')
			outputFile.append(os.path.join(outputFolder, "traffic_dq3_" + str(time.time()) + ".nc"))
			common.createNetCDFFileBusUsage(outputFile[3], x, y, times, measure_dq3,'passengers_timeliness')

	elif procType == "passengerUsage":
		time_period = 86400
		x = data[0]
		y = data[1]
		t = data[2]
		w = data[3]
		z = data[4]
		dq1 = data[5]
		dq2 = data[6]
		dq3 = data[7]

		diff_y = [y[i] != y[i+1] for i in range(0,len(y)-1)]
		diff_x = [x[i] != x[i+1] for i in range(0,len(x)-1)]
		diff = numpy.logical_or(diff_x, diff_y)

		t = pandas.to_datetime(t, format='%d/%m/%y %H:%M:%S,%f')

		#Split time array based on external dimensions
		records_split = numpy.where(diff)[0]+1
		sub_times = numpy.split(t, records_split)
		if dq1 is not None and dq2 is not None and dq3 is not None:
			sub_dq1 = numpy.split(dq1, records_split)
			sub_dq2 = numpy.split(dq2, records_split)
			sub_dq3 = numpy.split(dq3, records_split)
		#Add index for first element
		records_split = numpy.insert(records_split,0,0)
		sub_x = numpy.take(x, records_split)
		sub_y = numpy.take(y, records_split)

		x = numpy.unique(x)
		y = numpy.unique(y)

		#Define partitions for concurrent execution
		if int(multiProcesses) > 1:
			#Compute exact number or records per task
			minNum = math.floor(len(t) / float(multiProcesses))
			remainder = len(t) % minNum
			record_task = []
			for i in range(0,int(multiProcesses)):
				if remainder > 0:
					record_task.append(minNum +1)
					remainder = remainder - 1
				else:
					record_task.append(minNum)

			#Compute number of matrix rows per task
			row_task = []
			count = 0
			for i in diff_x:
				count = count + 1
				if i == True:
					row_task.append(count)
					count = 0
			#Append last
			count = count + 1
			row_task.append(count)

			#Compute number of sub_times per task based on row
			partitions = []
			count = 0
			for i in range(0,int(multiProcesses)):
				while row_task:
					count = count + row_task.pop(0)
					if count >= record_task[i] or not row_task:
						partitions.append(count)
						count = 0
						break

			#Split arrays
			count = 0
			current = 0
			splits = []
			for p in partitions:
				for i in range(current,len(sub_times)):
					count = count + len(sub_times[i])
					current = current + 1
					if count == p or not sub_times:
						splits.append(current)
						count = 0
						break

			sub_times = numpy.array_split(sub_times, splits)
			sub_x = numpy.array_split(sub_x, splits)
			sub_y = numpy.array_split(sub_y, splits)
			if dq1 is not None and dq2 is not None and dq3 is not None:
				sub_dq1 = numpy.array_split(sub_dq1, splits)
				sub_dq2 = numpy.array_split(sub_dq2, splits)
				sub_dq3 = numpy.array_split(sub_dq3, splits)

			threadNum = len(splits) if len(splits) < multiProcesses else multiProcesses
		else:
			sub_times = [sub_times]
			sub_x = [sub_x]
			sub_y = [sub_y]
			if dq1 is not None and dq2 is not None and dq3 is not None:
				sub_dq1 = [sub_dq1]
				sub_dq2 = [sub_dq2]
				sub_dq3 = [sub_dq3]
			threadNum = 1

		#Define time dimension (aggregate on time period)
		start_date = min(t)
		end_date = max(t)
		interval = end_date.date() - start_date.date()
		start_time = calendar.timegm(start_date.date().timetuple())
		time_len = (interval.days + 1)*int((24*3600)/time_period)
		#Time val contains also 24 steps for final day
		time_val = [start_time + i*time_period for i in range(0,time_len+1)]

		results = [0 for i in range(0, int(threadNum))]
		if dq1 is not None and dq2 is not None and dq3 is not None:
			results_dq1 = [0 for i in range(0, int(threadNum))]
			results_dq2 = [0 for i in range(0, int(threadNum))]
			results_dq3 = [0 for i in range(0, int(threadNum))]

		for i in range(0, int(threadNum)):
			if mode == 'compss':
				if dq1 is not None and dq2 is not None and dq3 is not None:
					from compss_functions import compssTransformDQ
					results[i] = compssTransformDQ(sub_x[i], sub_y[i], sub_times[i], sub_dq1[i], sub_dq2[i], sub_dq3[i], x, y, time_val)
				else:
					from compss_functions import compssTransform
					results[i] = compssTransform(sub_x[i], sub_y[i], sub_times[i], x, y, time_val)
			else:
				if dq1 is not None and dq2 is not None and dq3 is not None:
					from internal_functions import internalTransformDQ
					res = internalTransformDQ(sub_x[i], sub_y[i], sub_times[i], sub_dq1[i], sub_dq2[i], sub_dq3[i], x, y, time_val)
					results[i] = res[0]
					results_dq1[i] = res[1]
					results_dq2[i] = res[2]
					results_dq3[i] = res[3]
				else:
					from internal_functions import internalTransform
					results[i] = internalTransform(sub_x[i], sub_y[i], sub_times[i], x, y, time_val)

		if mode == 'compss':
			from pycompss.api.api import compss_wait_on
			results = compss_wait_on(results)
			if dq1 is not None and dq2 is not None and dq3 is not None:
				res = results[:]
				for i in range(len(res)):
					results[i] = res[i][0]
					results_dq1[i] = res[i][1]
					results_dq2[i] = res[i][2]
					results_dq3[i] = res[i][3]

		resultList = []
		for r in results:
			resultList.append(r)

		measure = numpy.concatenate(resultList, axis=0)

		if dq1 is not None and dq2 is not None and dq3 is not None:
			resultList = []
			for r in results_dq1:
				resultList.append(r)
			measure_dq1 = numpy.concatenate(resultList, axis=0)
			resultList = []
			for r in results_dq2:
				resultList.append(r)
			measure_dq2 = numpy.concatenate(resultList, axis=0)
			resultList = []
			for r in results_dq3:
				resultList.append(r)
			measure_dq3 = numpy.concatenate(resultList, axis=0)

		#Match extra attributes with unique users
		#x = [p for p,v in enumerate(x)]
		w = pandas.to_datetime(w, format='%d/%m/%y', errors='coerce')
		w = [k if k < numpy.datetime64('2018-01-01') else k.replace(year=k.year-100) for k in w]
		records_split = numpy.where(diff_x)[0]+1
		records_split = numpy.insert(records_split,0,0)
		sub_w = numpy.take(w, records_split)
		sub_z = numpy.take(z, records_split)

		#Convert extra attributes to integer (when value is available)
		for p,v in enumerate(x):
			if sub_w[p] is not pandas.NaT:
				x[p] = int(str(int(time.mktime(sub_w[p].timetuple()))) + str(1 if sub_z[p] == "F" else 2))
			else:
				x[p] = 0

		#Create NetCDF file
		start_time = datetime.datetime.strptime(datetime.datetime.utcfromtimestamp(time_val[0]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
		times = [start_time + datetime.timedelta(days=0.5) + n *datetime.timedelta(days=1) for n in range(time_len)]
		outputFile = []
		outputFile.append(os.path.join(outputFolder, "traffic_" + str(time.time()) + ".nc"))
		common.createNetCDFFilePassengerUsage(outputFile[0], x, y, times, measure,'usage')

		if dq1 is not None and dq2 is not None and dq3 is not None:
			outputFile.append(os.path.join(outputFolder, "traffic_dq1_" + str(time.time()) + ".nc"))
			common.createNetCDFFilePassengerUsage(outputFile[1], x, y, times, measure_dq1,'usage_completeness')
			outputFile.append(os.path.join(outputFolder, "traffic_dq2_" + str(time.time()) + ".nc"))
			common.createNetCDFFilePassengerUsage(outputFile[2], x, y, times, measure_dq2,'usage_consistency')
			outputFile.append(os.path.join(outputFolder, "traffic_dq3_" + str(time.time()) + ".nc"))
			common.createNetCDFFilePassengerUsage(outputFile[3], x, y, times, measure_dq3,'usage_timeliness')

	elif procType == "busStops":
		time_period = 3600
		x = data[0]
		y = data[1]
		t = data[2]
		m = data[3]

		diff_y = [y[i] != y[i+1] for i in range(0,len(y)-1)]
		diff_x = [x[i] != x[i+1] for i in range(0,len(x)-1)]
		diff = numpy.logical_or(diff_x, diff_y)

		t = pandas.to_datetime(t, format='%d/%m/%y %H:%M:%S')

		#Split time array based on external dimensions
		records_split = numpy.where(diff)[0]+1
		sub_times = numpy.split(t, records_split)
		sub_m = numpy.split(m, records_split)
		#Add index for first element
		records_split = numpy.insert(records_split,0,0)
		sub_x = numpy.take(x, records_split)
		sub_y = numpy.take(y, records_split)

		x = numpy.unique(x)
		y = numpy.unique(y)

		#Define partitions for concurrent execution
		if int(multiProcesses) > 1:
			#Compute exact number or records per task
			minNum = math.floor(len(t) / float(multiProcesses))
			remainder = len(t) % minNum
			record_task = []
			for i in range(0,int(multiProcesses)):
				if remainder > 0:
					record_task.append(minNum +1)
					remainder = remainder - 1
				else:
					record_task.append(minNum)

			#Compute number of matrix rows per task
			row_task = []
			count = 0
			for i in diff_x:
				count = count + 1
				if i == True:
					row_task.append(count)
					count = 0
			#Append last
			count = count + 1
			row_task.append(count)

			#Compute number of sub_times per task based on row
			partitions = []
			count = 0
			for i in range(0,int(multiProcesses)):
				while row_task:
					count = count + row_task.pop(0)
					if count >= record_task[i] or not row_task:
						partitions.append(count)
						count = 0
						break

			#Split arrays
			count = 0
			current = 0
			splits = []
			for p in partitions:
				for i in range(current,len(sub_times)):
					count = count + len(sub_times[i])
					current = current + 1
					if count == p or not sub_times:
						splits.append(current)
						count = 0
						break

			sub_times = numpy.array_split(sub_times, splits)
			sub_m = numpy.array_split(sub_m, splits)
			sub_x = numpy.array_split(sub_x, splits)
			sub_y = numpy.array_split(sub_y, splits)
			threadNum = len(splits) if len(splits) < multiProcesses else multiProcesses
		else:
			sub_times = [sub_times]
			sub_m = [sub_m]
			sub_x = [sub_x]
			sub_y = [sub_y]
			threadNum = 1

		#Define time dimension (aggregate on time period)
		start_date = min(t)
		end_date = max(t)
		interval = end_date.date() - start_date.date()
		start_time = calendar.timegm(start_date.date().timetuple())
		time_len = (interval.days + 1)*int((24*3600)/time_period)
		#Time val contains also 24 steps for final day
		time_val = [start_time + i*time_period for i in range(0,time_len+1)]

		results = [0 for i in range(0, int(threadNum))]

		for i in range(0, int(threadNum)):
			if mode == 'compss':
				from compss_functions import compssEMTransform
				results[i] = compssEMTransform(sub_x[i], sub_y[i], sub_times[i], sub_m[i], x, y, time_val)
			else:
				from internal_functions import internalEMTransform
				results[i] = internalEMTransform(sub_x[i], sub_y[i], sub_times[i], sub_m[i], x, y, time_val)

		if mode == 'compss':
			from pycompss.api.api import compss_wait_on
			results = compss_wait_on(results)

		resultList = []
		for r in results:
			resultList.append(r)

		measure = numpy.concatenate(resultList, axis=0)

		#Create NetCDF file
		start_time = datetime.datetime.strptime(datetime.datetime.utcfromtimestamp(time_val[0]).strftime('%Y-%m-%d %H:%M:%S'), "%Y-%m-%d %H:%M:%S")
		times = [start_time + datetime.timedelta(hours=0.5) + n *datetime.timedelta(hours=1) for n in range(time_len)]
		outputFile = []
		outputFile.append(os.path.join(outputFolder, "traffic_" + str(time.time()) + ".nc"))
		common.createNetCDFFileEMBus(outputFile[0], x, y, times, measure,'passengers')
	else:
		raise RuntimeError("Type of processing not recognized")

	return times, outputFile

def loadOphidia(fileRef, times, singleNcores, user, password, hostname, port, procType, distribution, logFlag):

	if procType == "busUsage":
		measure = "passengers"
		dq_measures = ["passengers_completeness", "passengers_consistency", "passengers_timeliness"]
		imp_concept_level = "h"
	elif procType == "passengerUsage":
		measure = "usage"
		dq_measures = ["usage_completeness", "usage_consistency", "usage_timeliness"]
		imp_concept_level = "d"
	elif procType == "busStops":
		measure = "passengers"
		imp_concept_level = "h"
	else:
		raise RuntimeError("Type of processing not recognized")

	sys.stdout = open(os.devnull, 'w')

	if user is "__TOKEN__":
		cube.Cube.setclient(token=password, server=hostname, port=port)
	else:
		cube.Cube.setclient(username=user, password=password, server=hostname, port=port)

	if logFlag == True:
		import timeit
		import logging
		import inspect
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()

	if len(fileRef) != 1 and len(fileRef) != 4:
		raise RuntimeError("Number of input files is not correct")

	if distribution in "distributed":
		cube.Cube.script(script='bigsea_retrieve',args=fileRef[0]+'|token',display=False)
		data = json.loads(cube.Cube.client.last_response)
		inputFile = data['response'][0]['objcontent'][0]["message"].splitlines()[0]
	else:
		inputFile = fileRef[0]

	dq_files = []
	if len(fileRef) == 4:
		for f in fileRef[1:]:
			if distribution in "distributed":
				cube.Cube.script(script='bigsea_retrieve',args=f+'|token',display=False)
				data = json.loads(cube.Cube.client.last_response)
				inputF = data['response'][0]['objcontent'][0]["message"].splitlines()[0]
			else:
				inputF = f

			dq_files.append(inputF)

	#Check instance base_src_path
	if cube.Cube.client.base_src_path != "/" and inputFile.startswith(cube.Cube.client.base_src_path):
		inputFile = inputFile[len(cube.Cube.client.base_src_path):]

		if len(dq_files) > 0:
			for i in range(len(dq_files)):
				dq_files[i] = dq_files[i][len(cube.Cube.client.base_src_path):]

	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] SCRIPT execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	try:
		cube.Cube.createcontainer(container='bigsea',dim='cod_passenger|cod_linha|cod_veiculo|bus_stop|time',dim_type='long|long|long|long|double',hierarchy='oph_base|oph_base|oph_base|oph_base|oph_time',display=False,base_time='2015-01-01 00:00:00',calendar='gregorian',units='h')
	except:
		pass

	if logFlag == True:
		frame = inspect.getframeinfo(inspect.currentframe())
		start_time = timeit.default_timer()

	pid = []
	if procType == "busUsage":
		historicalCube = cube.Cube.importnc(container='bigsea', measure=measure, imp_dim='time', imp_concept_level=imp_concept_level, import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=inputFile, display=False, ncores=singleNcores, ioserver="ophidiaio_memory")
		pid.append(historicalCube.pid)
		if len(dq_files) > 0:
			for i in range(len(dq_files)):
				historicalCube_dq = cube.Cube.importnc(container='bigsea', measure=dq_measures[i], imp_dim='time', imp_concept_level=imp_concept_level, import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=dq_files[i], display=False, ncores=singleNcores, ioserver="ophidiaio_memory")
				pid.append(historicalCube_dq.pid)

	elif procType == "passengerUsage":
		historicalCube = cube.Cube.importnc(container='bigsea', measure=measure, exp_concept_level=imp_concept_level+'|c', import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=inputFile , display=False, ncores=singleNcores, ioserver="ophidiaio_memory")
		pid.append(historicalCube.pid)
		if len(dq_files) > 0:
			for i in range(len(dq_files)):
				historicalCube_dq = cube.Cube.importnc(container='bigsea', measure=dq_measures[i], exp_concept_level=imp_concept_level+'|c', import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=dq_files[i], display=False, ncores=singleNcores, ioserver="ophidiaio_memory")
				pid.append(historicalCube_dq.pid)

	elif procType == "busStops":
		historicalCube = cube.Cube.importnc(container='bigsea', measure=measure, imp_dim='time', imp_concept_level=imp_concept_level, import_metadata='no', base_time='2015-01-01 00:00:00', calendar='gregorian', units='h', src_path=inputFile, display=False, ncores=singleNcores, ioserver="ophidiaio_memory")
		pid.append(historicalCube.pid)

	if logFlag == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] IMPORTNC execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='datacube_name',metadata_value='historical_'+measure, display=False)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='start_date',metadata_value=str(times[0].date()), display=False)
	historicalCube.metadata(mode='insert',metadata_type='text',metadata_key='end_date',metadata_value=str(times[-1].date()), display=False)

	sys.stdout = sys.__stdout__;

	return pid

