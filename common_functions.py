import os, shutil, subprocess, csv, datetime 
import numpy, netCDF4

#Global lists of metrics being computed
METRICS = ['MIN', 'MAX', 'AVG', 'SUM']

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
		inNum = int(inNum/96)

	return outString[::-1]

def jsonLine2json(filename):

	inFilename, inFileExt = os.path.splitext(filename)

	outFileName = ""
	if inFileExt == '.txt':
		outFileName = inFilename + ".json"
	else:
		raise RuntimeError("Input file not valid")

	shutil.copyfile(filename, outFileName)

	with open(outFileName, 'r+') as outFile:
		wholeFile = outFile.read()
		outFile.seek(0)
		outFile.write("[\n" + wholeFile + ']')

	return outFileName


def anonymizeFile(anonymizationBin, inputFile, policyFile):

	try:
		proc = subprocess.Popen(["java -jar " + anonymizationBin + " " + inputFile + " " + policyFile], cwd=os.path.dirname(os.path.abspath(__file__)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
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

	return anonymFile


def createJSONFile(outputFolder, fileName, passengerData, codLinhaData, dateData, mode, keepNan):

	jsonFile = os.path.join(outputFolder, fileName+'.json')
	with open(jsonFile, mode) as outfile:
		for l in range(0, len(passengerData[0])):
			line = {}
			if codLinhaData:
				line['CODLINHA'] = convertNumtoASCII(int(codLinhaData[l]))

			for c in range(0, len(passengerData[0][l])):
				line['DATETIME'] =  dateData[c]
				#In case only nans do not add line to output	
				stop_cond = 0

				for i, m in enumerate(passengerData):
					if keepNan == 0 and numpy.isnan(m[l][c]):
						stop_cond = 1
						break
					else:
						line[METRICS[i]] = round(m[l][c], 3)
						
				if stop_cond == 0:
					json.dump(line, outfile, sort_keys=True)
					outfile.write('\n')

	return jsonFile	

def createCSVFile(outputFolder, fileName, passengerData, codLinhaData, dateData, mode, keepNan):

	csvFile = os.path.join(outputFolder, fileName+'.csv')
	with open(csvFile, mode) as outfile:
		csvWriter = csv.writer(outfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
		if mode == 'w':
			header = []
			if codLinhaData:
				header.append("CODLINHA")
			header.append("DATETIME")
			for i in range(len(passengerData)):
				header.append(METRICS[i])

			csvWriter.writerow(header)

		for l in range(0, len(passengerData[0])):

			for c in range(0, len(passengerData[0][l])):
				row = []
				if codLinhaData:
					row.append(convertNumtoASCII(int(codLinhaData[l])))
				row.append(dateData[c])
				#In case only nans do not add line to output	
				stop_cond = 0

				for i, m in enumerate(passengerData):
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
			filterList = filterList + dayStart.strftime("%Y-%m-%d %H:%M:%S") + "_" + dayEnd.strftime("%Y-%m-%d %H:%M:%S") + ","

	if not filterList:
		return None 
	else:
		return filterList[:-1]


def createNetCDFFile(filename, cod_linha, cod_veiculo, times, measure):

	outnc = netCDF4.Dataset(filename, 'w', format='NETCDF4')
	time_dim = outnc.createDimension('time', len(times))
	line_dim = outnc.createDimension('cod_linha', None) # None means unlimited
	vehicle_dim = outnc.createDimension('cod_veiculo', len(cod_veiculo)) 
	time_var = outnc.createVariable('time', 'd', ('time',))
	line_var = outnc.createVariable('cod_linha', numpy.int64, ('cod_linha',))
	vehicle_var = outnc.createVariable('cod_veiculo', numpy.int64, ('cod_veiculo',))

	measure_var = outnc.createVariable('passengers', numpy.float32, ('cod_linha','cod_veiculo','time',), fill_value='NaN')

	#Set metadata
	time_var.units = 'hours since 2015-1-1 00:00:00' 
	time_var.calendar = 'gregorian'
	time_var.standard_name = 'time'
	time_var.long_name = 'time'
	time_var.axis = 'T'

	line_var.axis = "Y" ;
	vehicle_var.axis = "X" ;

	measure_var.standard_name = "passengers"
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

