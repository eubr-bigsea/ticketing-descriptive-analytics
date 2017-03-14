import os, sys, time, shutil
import threading

#Import internal dependencies
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common
import descriptive_stats as dstat
import ticketing_etl as etl

if __name__ == "__main__":

	#Script arguments 
	parallelNcores = 1
	singleNcores = 1
	multiProcesses = 1
	user=''
	password=''
	hostname=''
	port=''
	policyFile = ""
	anonymizationBin = ""

	inputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data")
	tmpFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_data")
	outputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_data")

	if not os.path.exists(tmpFolder):
	    os.makedirs(tmpFolder)

	print("*************************************************")

	for e in sorted(os.listdir(inputFolder)):
		inputFile = os.path.join(inputFolder, e)
		if os.path.isfile(inputFile):
			inFilename, inFileExt = os.path.splitext(inputFile)
			if inFileExt == '.txt':
				print("Anonymizing file: \"" + e + "\"")
				newFile = common.jsonLine2json(inputFile)
				anonymFile = common.anonymizeFile(anonymizationBin, newFile, policyFile)
				os.remove(newFile)
				path, name = os.path.split(anonymFile)
				outName = os.path.join(tmpFolder, name)
				shutil.move(anonymFile, outName)

	print("*************************************************\n")

	print("*************************************************")
	print("Starting ETL process")
	print("Running step 1 -> Extraction")

	line, vehicle, date = etl.extractFromFiles(tmpFolder)

	print("Running step 2 -> Transformation")

	times , outFile = etl.transformToNetCDF(line, vehicle, date, tmpFolder, multiProcesses)

	print("Running step 3 -> Loading")

	etl.loadOphidia(outFile, times, singleNcores, user, password, hostname, port)	
	
	print("End of ETL process")
	print("*************************************************\n")

	print("*************************************************")
	print("Running statistics computation")

	print("Computing: number of passenger stats for each hour of group of weekdays")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-peakhours", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and group of weekdays")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each hour of weekday")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-peakhours", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and weekday")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and hour of weekdays")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-hourly-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and hour of group of weekdays")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-hourly-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and month in the time range")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "monthly-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and week in the time range")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekly-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and day in the time range")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "daily-lines", "csv", outputFolder)

	print("Computing: number of passenger stats for each bus line and hour in the time range")
	dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "hourly-lines", "csv", outputFolder)

	print("*************************************************\n")

