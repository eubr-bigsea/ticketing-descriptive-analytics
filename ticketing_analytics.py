import os, sys, time, shutil, argparse
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common
import privacy_functions as privacy
import descriptive_stats as dstat
import ticketing_etl as etl
import ConfigParser

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

	if dataDir == "$PWD":
		inputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data")
		tmpFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_data")
		outputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_data")
		configFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
	else:
		inputFolder = os.path.join(dataDir, "input_data")
		tmpFolder = os.path.join(dataDir, "tmp_data")
		outputFolder = os.path.join(dataDir, "output_data")
		configFile = os.path.join(dataDir, "config.ini")

	#Read config. arguments 
	config = ConfigParser.ConfigParser()
	config.read(configFile)


	if config.has_option('main', 'parallelNcores'):
		parallelNcores = config.get('main', 'parallelNcores')
	else:
		parallelNcores = 1
	if config.has_option('main', 'singleNcores'):
		singleNcores = config.get('main', 'singleNcores')
	else:
		singleNcores = 1
	if config.has_option('main', 'multiProcesses'):
		multiProcesses = config.get('main', 'multiProcesses')
	else:
		multiProcesses = 1
	if config.has_option('main', 'benchmark'):
		benchmark = config.get('main', 'benchmark')
	else:
		benchmark = False

	if config.has_option('ophidia', 'user'):
		user = config.get('ophidia', 'user')
	else:
		print("Username for Ophidia instance not specified in configuration file")
		exit(1)
	if config.has_option('ophidia', 'pass'):
		password = config.get('ophidia', 'pass')
	else:
		print("Password for Ophidia instance not specified in configuration file")
		exit(1)
	if config.has_option('ophidia', 'host'):
		hostname = config.get('ophidia', 'host')
	else:
		print("Hostname of Ophidia instance not specified in configuration file")
		exit(1)
	if config.has_option('ophidia', 'port'):
		port = config.get('ophidia', 'port')
	else:
		port = '11732'

	if config.has_option('privacy', 'policyFile'):
		policyFile = config.get('privacy', 'policyFile')
	else:
		policyFile = "cards.json"
	if config.has_option('privacy', 'anonymizationBin'):
		anonymizationBin = config.get('privacy', 'anonymizationBin')
	else:
		anonymizationBin = "anonymization.jar"

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
