import os, shutil, subprocess, csv, datetime
import numpy, netCDF4, matplotlib.pyplot as plt

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
		#Divide by one position	
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


from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on


@task(anonymizationBin=IN, inputName=IN, inputFolder=IN, tmpFolder=IN, policyFile=IN, returns=str)
def anonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile):

	inputFile = os.path.join(inputFolder, inputName)
	if os.path.isfile(inputFile):
		inFilename, inFileExt = os.path.splitext(inputFile)
		if inFileExt == '.txt':
			print("Anonymizing file: \"" + inputName + "\"")
			newFile = jsonLine2json(inputFile)

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


def createJSONFile(outputFolder, fileName, passengerData, codLinhaData, dateData, mode, keepNan):

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

def createSimplePlot(csvFile, plotName):

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

