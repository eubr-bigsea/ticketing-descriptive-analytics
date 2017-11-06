import os, sys, time, shutil, argparse
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common
import privacy_functions as privacy
import descriptive_stats as dstat
import ticketing_etl as etl

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	subparsers = parser.add_subparsers(dest="type", help='Type of index to evaluate')

	parserA = subparsers.add_parser('bus-usage',description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserA.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "weekdaysets-peakhours", "weekdaysets-lines", "weekdays-peakhours", "weekdays-lines", "weekdays-hourly-lines", "weekdaysets-hourly-lines", "monthly-lines", "weekly-lines", "daily-lines", "hourly-lines"])
	parserA.add_argument('-f','--format', default="csv", help='Type of output format', choices=['json','csv'])
	parserA.add_argument('-d','--dir', default="$PWD", help='Base path of input_data, tmp_data and output_data')
	parserA.add_argument('-m','--mode', default="compss", help='Processing mode', choices=['compss','sequential'])

	parserB = subparsers.add_parser('passenger-usage', description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserB.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "monthly-usage", "weekly-usage"])
	parserB.add_argument('-f','--format', default="csv", help='Type of output format', choices=['json','csv'])
	parserB.add_argument('-d','--dir', default="$PWD", help='Base path of input_data, tmp_data and output_data')
	parserB.add_argument('-m','--mode', default="compss", help='Processing mode', choices=['compss','sequential'])

	args = parser.parse_args()

	if args.type == 'bus-usage':
		procType = "busUsage"
	elif args.type == 'passenger-usage':
		procType = "passengerUsage"

	stats = args.stats
	format = args.format
	dataDir = args.dir
	mode = args.mode

	#Define arguments
	parallelNcores = 1
	singleNcores = 1
	multiProcesses = 1
	user=''
	password=''
	hostname=''
	port=''
	policyFile = ""
	anonymizationBin = ""
	benchmark = False

	if dataDir == "$PWD":
		inputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data")
		tmpFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_data")
		outputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_data")
	else:
		inputFolder = os.path.join(dataDir, "input_data")
		tmpFolder = os.path.join(dataDir, "tmp_data")
		outputFolder = os.path.join(dataDir, "output_data")

	print time.strftime('%Y-%m-%d %H:%M:%S')

	#Create tmp folder
	if not os.path.exists(tmpFolder):
	    os.makedirs(tmpFolder)

	#Run anonymization
	print("*************************************************")

	if benchmark == True:
		import timeit
		global_start_time = timeit.default_timer()

	anonymFile = [0 for m in range(0,len(os.listdir(inputFolder)))]
	for i, e in enumerate(sorted(os.listdir(inputFolder))):
		if benchmark == True:
			start_time = timeit.default_timer()
		anonymFile[i] = privacy.anonymizeFile(anonymizationBin, e, inputFolder, tmpFolder, policyFile, mode)
		if benchmark == True:
			final_time = timeit.default_timer() - start_time
			print("Time required on file: "+ str(final_time))

	if benchmark == True:
		anonymFile = compss_wait_on(anonymFile)
		global_final_time = timeit.default_timer() - global_start_time
		print("Required time: "+ str(global_final_time))

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("*************************************************\n")

	print("*************************************************")
	print("Starting ETL process")
	print("Running step 1 -> Extraction")

	data = etl.extractPhase(anonymFile, tmpFolder, procType, mode)

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("Running step 2 -> Transformation")

	times , outFile = etl.transformToNetCDF(data, tmpFolder, multiProcesses, procType)

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("Running step 3 -> Loading")

	#Import into Ophidia
	etl.loadOphidia(outFile, times, singleNcores, user, password, hostname, port, procType)	

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("End of ETL process")
	print("*************************************************\n")

	#Run computations
	print("*************************************************")
	print("Running statistics computation")


	if procType == "passengerUsage":
		if stats == "all" or stats == "weekly-usage":
			print("Computing: passenger usage stats for each bus line and week in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekly-usage", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-usage":
			print("Computing: passenger usage stats for each bus line and month in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "monthly-usage", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

	elif procType == "busUsage":
		if stats == "all" or stats == "weekdaysets-peakhours":
			print("Computing: number of passenger stats for each hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-peakhours", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-lines":
			print("Computing: number of passenger stats for each bus line and group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-peakhours":
			print("Computing: number of passenger stats for each hour of weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-peakhours", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-lines":
			print("Computing: number of passenger stats for each bus line and weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-hourly-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-hourly-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-lines":
			print("Computing: number of passenger stats for each bus line and month in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "monthly-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekly-lines":
			print("Computing: number of passenger stats for each bus line and week in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekly-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "daily-lines":
			print("Computing: number of passenger stats for each bus line and day in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "daily-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "hourly-lines", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

	print("*************************************************\n")
