import os, shutil, subprocess, json, csv, calendar, datetime
import numpy, netCDF4

#Global lists of metrics being computed
METRICS_BUS = ['MIN', 'MAX', 'AVG', 'SUM']
METRICS_USER = ['MIN', 'MAX', 'COUNT', 'SUM']


def checkFormat(inputFile, format):
	with open(inputFile) as f:
		"""firstLine = f.readline()
		try:
			if format == "json":
				jsonLine = json.loads(firstLine)
			elif format == "csv":
				csvreader = csv.reader(firstLine, delimiter=",")
		except ValueError, e:
			print("File " + inputFile + " is not in "+format)
			return False"""

		return True

	return False

def getFiles(inputFolder):
	#Count input files
	file_num = 0
	file_list = []
	for root, dirs, files in os.walk(inputFolder):
		for f in files:
			if f != "_SUCCESS" and not f.startswith('.') and not f.startswith("tmp_") and not f.endswith("_anonymized.json"):
				file_num = file_num + 1
				file_list.append(root+"/"+f)

	return file_num, file_list

def convertASCIItoNum(inString):

	totLen = len(inString)
	outNum = 0
	for i in range(len(inString)):
		#Get number of char and shift it of 32 (unused chars)
		numText = (ord(inString[i]) - 32)
		#Add char number elevated by reverse of position
		outNum = outNum + (96**(totLen - (i+1)))*numText

	return outNum

def convertNumtoASCII(inNum):

	outString = ""
	while(inNum > 0):
		#Compute residual of position len - i
		r = int(inNum%96)
		#Concatenate char to string (add 32 removed in coding)
		outString = outString + chr(r+32)
		#Divide by one position
		inNum = int(inNum/96)

	return outString[::-1]

def jsonLine2json(filename):

	inFile, inFileExt = os.path.splitext(filename)
	inFilename = os.path.basename(inFile)
	inFilepath = os.path.dirname(inFile)
	outFileName = os.path.join(inFilepath, "tmp_" + inFilename + ".json")

	shutil.copyfile(filename, outFileName)

	with open(outFileName, 'r+') as outFile:
		wholeFile = outFile.read()
		outFile.seek(0)
		outFile.write("[\n" + wholeFile + ']')

	return outFileName


def aggregateData(args):
	ar = args[0]
	time_val = args[1]
	if len(args) == 3:
		values = args[2]
	else:
		values = None
	measure = numpy.full([len(time_val)-1],numpy.nan, dtype=numpy.float32)
	time = [calendar.timegm(k.timetuple()) for k in ar]

	if values is not None:
		for idx,t in enumerate(time):
			for time_index in range(0,len(time_val)-1):
				if t >= time_val[time_index] and t < time_val[time_index+1]:
					if not numpy.isnan(measure[time_index]):
						measure[time_index] += values[idx]
					else:
						measure[time_index] = values[idx]
					break
	else:
		for idx,t in enumerate(time):
			for time_index in range(0,len(time_val)-1):
				if t >= time_val[time_index] and t < time_val[time_index+1]:
					if not numpy.isnan(measure[time_index]):
						measure[time_index] += 1
					else:
						measure[time_index] = 1
					break

	return measure

def aggregateDataDQ(args):
	ar = args[0]
	time_val = args[1]

	#DQ metrics
	dq1 = args[2]
	dq2 = args[3]
	dq3 = args[4]

	measure = numpy.full([len(time_val)-1],numpy.nan, dtype=numpy.float32)
	measure_dq1 = numpy.full([len(time_val)-1],numpy.nan, dtype=numpy.float32)
	measure_dq2 = numpy.full([len(time_val)-1],numpy.nan, dtype=numpy.float32)
	measure_dq3 = numpy.full([len(time_val)-1],numpy.nan, dtype=numpy.float32)
	time = [calendar.timegm(k.timetuple()) for k in ar]
	for idx, t in enumerate(time):
		for time_index in range(0,len(time_val)-1):
			if t >= time_val[time_index] and t < time_val[time_index+1]:
				if not numpy.isnan(measure[time_index]):
					measure[time_index] += 1
					measure_dq1[time_index] += dq1[idx]
					measure_dq2[time_index] += dq2[idx]
					measure_dq3[time_index] = numpy.min([dq3[idx], measure_dq3[time_index]])
				else:
					measure[time_index] = 1
					measure_dq1[time_index] = dq1[idx]
					measure_dq2[time_index] = dq2[idx]
					measure_dq3[time_index] = dq3[idx]
				break

	measure_dq1 = numpy.divide(measure_dq1, measure, out=numpy.full_like(measure,numpy.nan))
	measure_dq2 = numpy.divide(measure_dq2, measure, out=numpy.full_like(measure,numpy.nan))

	return [measure, measure_dq1, measure_dq2, measure_dq3]

def createJSONFileBusUsage(outputFolder, fileName, passengerData, codLinhaData, dateData, mode, keepNan):

	jsonFile = os.path.join(outputFolder, fileName+'.json')
	with open(jsonFile, mode) as outfile:
		#Loop on bus lines
		for l in range(0, len(passengerData[0])):
			line = {}
			if codLinhaData:
				line['CODLINHA'] = convertNumtoASCII(int(codLinhaData[l]))
			#Loop on time
			for c in range(0, len(passengerData[0][l])):
				line['DATETIME'] =  dateData[c]
				#In case only nans do not add line to output
				stop_cond = 0
				#Loop on measure
				for i, m in enumerate(passengerData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						line[METRICS_BUS[i]] = round(m[l][c], 3)

				if stop_cond == 0:
					json.dump(line, outfile, sort_keys=True)
					outfile.write('\n')

	return jsonFile

def createJSONFileBusStops(outputFolder, fileName, passengerData, busStopsData, dateData, mode, keepNan):

	jsonFile = os.path.join(outputFolder, fileName+'.json')
	with open(jsonFile, mode) as outfile:
		#Loop on bus lines
		for l in range(0, len(passengerData[0])):
			line = {}
			if busStopsData:
				line['BUSSTOPID'] = int(busStopsData[l])
			#Loop on time
			for c in range(0, len(passengerData[0][l])):
				line['DATETIME'] =  dateData[c]
				#In case only nans do not add line to output
				stop_cond = 0
				#Loop on measure
				for i, m in enumerate(passengerData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						line[METRICS_BUS[i]] = round(m[l][c], 3)

				if stop_cond == 0:
					json.dump(line, outfile, sort_keys=True)
					outfile.write('\n')

	return jsonFile

def createJSONFilePassengerUsage(outputFolder, fileName, usageData, passengerData, dateData, mode, keepNan):

	jsonFile = os.path.join(outputFolder, fileName+'.json')
	with open(jsonFile, mode) as outfile:
		#Loop on bus users
		for l in range(0, len(usageData[0])):
			line = {}
			if passengerData:
				line['BIRTHDATE'] = passengerData[l][0]
				line['GENDER'] = passengerData[l][1]
			#Loop on time
			for c in range(0, len(usageData[0][l])):
				line['DATETIME'] =  dateData[c]
				#In case only nans do not add line to output
				stop_cond = 0
				#Loop on measure
				for i, m in enumerate(usageData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						line[METRICS_USER[i]] = round(m[l][c], 3)

				if stop_cond == 0:
					json.dump(line, outfile, sort_keys=True)
					outfile.write('\n')

	return jsonFile

def createCSVFileBusUsage(outputFolder, fileName, passengerData, codLinhaData, dateData, mode, keepNan):

	csvFile = os.path.join(outputFolder, fileName+'.csv')
	with open(csvFile, mode) as outfile:
		csvWriter = csv.writer(outfile, delimiter=';',quotechar='"', quoting=csv.QUOTE_MINIMAL)
		if mode == 'w':
			header = []
			if codLinhaData:
				header.append("CODLINHA")
			header.append("DATETIME")
			for i in range(len(passengerData)):
				header.append(METRICS_BUS[i])

			csvWriter.writerow(header)

		#Loop on bus lines
		for l in range(0, len(passengerData[0])):
			#Loop on time
			for c in range(0, len(passengerData[0][l])):
				row = []
				if codLinhaData:
					row.append(convertNumtoASCII(int(codLinhaData[l])))
				row.append(dateData[c])
				#In case only nans do not add line to output
				stop_cond = 0
				#Loop on measure
				for i, m in enumerate(passengerData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						row.append(round(m[l][c], 3))

				if stop_cond == 0:
					csvWriter.writerow(row)

	return csvFile

def createCSVFileBusStops(outputFolder, fileName, passengerData, busStopsData, dateData, mode, keepNan):

	csvFile = os.path.join(outputFolder, fileName+'.csv')
	with open(csvFile, mode) as outfile:
		csvWriter = csv.writer(outfile, delimiter=';',quotechar='"', quoting=csv.QUOTE_MINIMAL)
		if mode == 'w':
			header = []
			if busStopsData:
				header.append("BUSSTOPID")
			header.append("DATETIME")
			for i in range(len(passengerData)):
				header.append(METRICS_BUS[i])

			csvWriter.writerow(header)

		#Loop on bus lines
		for l in range(0, len(passengerData[0])):
			#Loop on time
			for c in range(0, len(passengerData[0][l])):
				row = []
				if busStopsData:
					row.append(int(busStopsData[l]))
				row.append(dateData[c])
				#In case only nans do not add line to output
				stop_cond = 0
				#Loop on measure
				for i, m in enumerate(passengerData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						row.append(round(m[l][c], 3))

				if stop_cond == 0:
					csvWriter.writerow(row)

	return csvFile

def createCSVFilePassengerUsage(outputFolder, fileName,  usageData, passengerData, dateData, mode, keepNan):

	csvFile = os.path.join(outputFolder, fileName+'.csv')
	with open(csvFile, mode) as outfile:
		csvWriter = csv.writer(outfile, delimiter=';',quotechar='"', quoting=csv.QUOTE_MINIMAL)
		if mode == 'w':
			header = []
			if passengerData:
				header.append("BIRTHDATE")
				header.append("GENDER")
			header.append("DATETIME")
			for i in range(len(usageData)):
				header.append(METRICS_USER[i])

			csvWriter.writerow(header)

		#Loop on bus users
		for l in range(0, len(usageData[0])):
			#Loop on time
			for c in range(0, len(usageData[0][l])):
				row = []
				if passengerData:
					row.append(passengerData[l][0])
					row.append(passengerData[l][1])
				row.append(dateData[c])
				#In case only nans do not add line to output
				stop_cond = 0
				#Loop on measure
				for i, m in enumerate(usageData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						row.append(round(m[l][c], 3))

				if stop_cond == 0:
					csvWriter.writerow(row)

	return csvFile

def buildSubsetFilter(startDate, numDays, day):

	filterList = ""
	for d in (startDate + n *datetime.timedelta(days=1) for n in range(0, numDays)):
		if d.isoweekday() == day:
			dayStart = d.replace(hour=00, minute=1)
			dayEnd = dayStart + datetime.timedelta(days=1)
			filterList = filterList + "{0:.3f}".format(netCDF4.date2num(dayStart, units = 'hours since 2015-1-1 00:00:00', calendar = 'gregorian')) + ":" + "{0:.3f}".format(netCDF4.date2num(dayEnd, units = 'hours since 2015-1-1 00:00:00', calendar = 'gregorian')) + ","

	if not filterList:
		return None
	else:
		return filterList[:-1]


def createNetCDFFileBusUsage(filename, cod_linha, cod_veiculo, times, measure, measure_name):

	outnc = netCDF4.Dataset(filename, 'w', format='NETCDF4')
	time_dim = outnc.createDimension('time', len(times))
	line_dim = outnc.createDimension('cod_linha', None) # None means unlimited
	vehicle_dim = outnc.createDimension('cod_veiculo', len(cod_veiculo))
	time_var = outnc.createVariable('time', 'd', ('time',))
	line_var = outnc.createVariable('cod_linha', numpy.int64, ('cod_linha',))
	vehicle_var = outnc.createVariable('cod_veiculo', numpy.int64, ('cod_veiculo',))

	measure_var = outnc.createVariable(measure_name, numpy.float32, ('cod_linha','cod_veiculo','time',), fill_value='NaN')

	#Set metadata
	time_var.units = 'hours since 2015-1-1 00:00:00'
	time_var.calendar = 'gregorian'
	time_var.standard_name = 'time'
	time_var.long_name = 'time'
	time_var.axis = 'T'

	line_var.axis = "Y" ;
	vehicle_var.axis = "X" ;

	measure_var.standard_name = measure_name
	measure_var.long_name = "Passenger count"
	measure_var.missing_value = "NaN"

	time_var[:] = netCDF4.date2num(times, units = time_var.units, calendar = time_var.calendar)
	line_var[:] = [convertASCIItoNum(str(t)) for t in cod_linha]
	vehicle_var[:] = [convertASCIItoNum(str(t)) for t in cod_veiculo]
	measure_var[:] = measure

	#Add metadata
	outnc.description = "Passenger count file"
	outnc.cmor_version = "0.96f"
	outnc.Conventions = "CF-1.0"
	outnc.frequency = "hourly"

	outnc.close()

def createNetCDFFileEMBus(filename, bus_stop, cod_linha, times, measure, measure_name):

	outnc = netCDF4.Dataset(filename, 'w', format='NETCDF4')
	time_dim = outnc.createDimension('time', len(times))
	busStop_dim = outnc.createDimension('bus_stop', None) # None means unlimited
	line_dim = outnc.createDimension('cod_linha', len(cod_linha))
	time_var = outnc.createVariable('time', 'd', ('time',))
	busStop_var = outnc.createVariable('bus_stop', numpy.int64, ('bus_stop',))
	line_var = outnc.createVariable('cod_linha', numpy.int64, ('cod_linha',))

	measure_var = outnc.createVariable(measure_name, numpy.float32, ('bus_stop','cod_linha','time',), fill_value='NaN')

	#Set metadata
	time_var.units = 'hours since 2015-1-1 00:00:00'
	time_var.calendar = 'gregorian'
	time_var.standard_name = 'time'
	time_var.long_name = 'time'
	time_var.axis = 'T'

	busStop_var.axis = "Y" ;
	line_var.axis = "X" ;

	measure_var.standard_name = measure_name
	measure_var.long_name = "Passenger count"
	measure_var.missing_value = "NaN"

	time_var[:] = netCDF4.date2num(times, units = time_var.units, calendar = time_var.calendar)
	busStop_var[:] = [int(t) for t in bus_stop]
	line_var[:] = [convertASCIItoNum(str(t)) for t in cod_linha]
	measure_var[:] = measure

	#Add metadata
	outnc.description = "Passenger count file"
	outnc.cmor_version = "0.96f"
	outnc.Conventions = "CF-1.0"
	outnc.frequency = "hourly"

	outnc.close()

def createNetCDFFilePassengerUsage(filename, cod_passenger, cod_linha, times, measure, measure_name):

	outnc = netCDF4.Dataset(filename, 'w', format='NETCDF4')
	time_dim = outnc.createDimension('time', None) # None means unlimited
	passenger_dim = outnc.createDimension('cod_passenger', len(cod_passenger)) # None means unlimited
	if cod_linha is not None:
		line_dim = outnc.createDimension('cod_linha', len(cod_linha))
		line_var = outnc.createVariable('cod_linha', numpy.int64, ('cod_linha',))
	time_var = outnc.createVariable('time', 'd', ('time',))
	passenger_var = outnc.createVariable('cod_passenger', numpy.int64, ('cod_passenger',))

	if cod_linha is not None:
		measure_var = outnc.createVariable(measure_name, numpy.float32, ('time','cod_linha','cod_passenger',), fill_value='NaN', chunksizes=(int(numpy.ceil(len(times)/10.0)), len(cod_linha), len(cod_passenger)))
	else:
		measure_var = outnc.createVariable(measure_name, numpy.float32, ('time','cod_passenger',), fill_value='NaN')

	#Set metadata
	time_var.units = 'hours since 2015-1-1 00:00:00'
	time_var.calendar = 'gregorian'
	time_var.standard_name = 'time'
	time_var.long_name = 'time'
	time_var.axis = 'T'

	if cod_linha is not None:
		line_var.axis = "X" ;
	passenger_var.axis = "Y" ;

	measure_var.standard_name = measure_name
	measure_var.long_name = "Bus usage count"
	measure_var.missing_value = "NaN"

	#Transpose matrix
	if cod_linha is not None:
		measure = measure.transpose((2,1,0))
	else:
		measure = measure.transpose((1,0))

	time_var[:] = netCDF4.date2num(times, units = time_var.units, calendar = time_var.calendar)
	if cod_linha is not None:
		line_var[:] = [convertASCIItoNum(str(t)) for t in cod_linha]
	passenger_var[:] = [t for t in cod_passenger]
	for i in range(len(times)):
		measure_var[i] = measure[i]

	#Add metadata
	outnc.description = "Bus usage file"
	outnc.cmor_version = "0.96f"
	outnc.Conventions = "CF-1.0"
	outnc.frequency = "daily"

	outnc.close()

def createSimplePlot(csvFile, plotName):
	import matplotlib.pyplot as plt

	with open(csvFile, 'r') as outfile:
		csvReader = csv.reader(outfile, delimiter=',', quotechar='"')
		plot_data = numpy.full([3,24],numpy.nan, dtype=numpy.float32)
		plot_dim = [i for i in range(24)]
		plot_name = ['Saturday/Sunday', 'Monday/Friday', 'Tuesday/Wednesday/Thursday']
		for row in csvReader:
			for i, n in enumerate(plot_name):
				if n in row[0]:
					index = int((row[0].split(n +" ",1)[1]).split('-')[0])
					plot_data[i][index] = float(row[4])

		fig = plt.figure()
		fig.set_size_inches(20, 10)
		ax = plt.subplot(111)
		ax.plot(plot_dim, plot_data[0], label=plot_name[0])
		ax.plot(plot_dim, plot_data[1], label=plot_name[1])
		ax.plot(plot_dim, plot_data[2], label=plot_name[2])
		plt.grid()
		plt.yticks(fontsize=14)
		plt.xticks(plot_dim,fontsize=14)
		handles, labels = ax.get_legend_handles_labels()
		ax.legend(handles, labels, loc='upper right',fontsize=16)
		plt.title("Total number of passengers per hour by group of weekdays",fontsize=22)
		plt.xlabel("Hour of day",fontsize=18)
		plt.ylabel("Number of passengers",fontsize=18)
		fig.savefig(plotName)
		#fig.show()

