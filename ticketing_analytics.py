import os, sys, time, shutil, argparse
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common
import privacy_functions as privacy
import descriptive_stats as dstat
import ticketing_etl as etl
import ConfigParser
import tarfile

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parser.add_argument('-u','--username', help='Username')
	parser.add_argument('-p','--password', help='Password of user')
	parser.add_argument('-t','--token', help='Access token')
	parser.add_argument('-f','--format', default="csv", help='Type of output format', choices=['json','csv'])
	parser.add_argument('-c','--conf', default="config.ini", help='Absolute path to config file')
	parser.add_argument('-m','--mode', default="compss", help='Processing mode', choices=['compss','sequential'])
	parser.add_argument('-d','--distribution', default="distributed", help='If the components are distributed or not', choices=['distributed','local'])
	parser.add_argument('-i','--input_cube', default="None", help='The ID of the input datecube in case no ETL has to be performed. In case of application of DQ filters, 4 cubes must be specified in the format: base cube PID, completeness cube PID, consistency cube PID, timeliness cube PID')
	parser.add_argument('-q','--dq_filters', default="None", help='Data quality filters in the format: completeness, consistency, timeliness. E.g. 0.5, 0.5, 0.5 or 0.5, None, 0.5')
	subparsers = parser.add_subparsers(dest="type", help='Type of index to evaluate')

	parserA = subparsers.add_parser('bus-usage',description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserA.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "weekdaysets-peakhours", "weekdaysets-lines", "weekdays-peakhours", "weekdays-lines", "weekdays-hourly-lines", "weekdaysets-hourly-lines", "monthly-lines", "weekly-lines", "daily-lines", "hourly-lines"])

	parserB = subparsers.add_parser('passenger-usage', description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserB.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "monthly-usage", "weekly-usage"])

	args = parser.parse_args()

	if args.type == 'bus-usage':
		procType = "busUsage"
	elif args.type == 'passenger-usage':
		procType = "passengerUsage"

	stats = args.stats
	format = args.format
	confFile = args.conf
	mode = args.mode
	distribution = args.distribution

	user = args.username
	password = args.password
	token = args.token

	#Check data quality filters
	dq_filters = args.dq_filters
	if dq_filters == "None":
		dq_filters = [None, None, None]
		dq_flag = False
	else:
		filter_count = 1 + dq_filters.count(',')
		if filter_count != 3:
			print("Data Quality filters format is not setup correctly")
			exit(1)
		else:
			import re
			dq_flag = True
			filter_list = dq_filters.split(",")
			dq_filters = []
			pattern = re.compile("^\d+(\.\d+)?$")
			for l in filter_list:
				if l == "None":
					dq_filters.append(None)
				elif pattern.match(l):
					dq_filters.append(float(l))
				else:
					print("Data Quality filters values are not setup correctly")
					exit(1)

	#Match datacube PIDs
	input_cubes = args.input_cube
	if input_cubes == "None":
		input_cubes = False
	else:
		cube_count = 1 + input_cubes.count(',')
		if cube_count != 1 and cube_count != 4:
			print("Input cubes are not setup correctly")
			exit(1)
		elif cube_count == 1:
			cube_list = [input_cubes]
			#If single cube is provided then no DQ filters are allowed
			dq_flag = False
		else:
			cube_list = input_cubes.split(",")

	if confFile == "config.ini":
		configFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
	else:
		configFile = confFile

	#Read config. arguments 
	config = ConfigParser.ConfigParser()
	config.read(configFile)

	if config.has_option('main', 'parallelNcores') and config.get('main', 'parallelNcores'):
		parallelNcores = config.get('main', 'parallelNcores')
	else:
		parallelNcores = 1
	if config.has_option('main', 'singleNcores') and config.get('main', 'singleNcores'):
		singleNcores = config.get('main', 'singleNcores')
	else:
		singleNcores = 1
	if config.has_option('main', 'multiProcesses') and config.get('main', 'multiProcesses'):
		multiProcesses = config.get('main', 'multiProcesses')
	else:
		multiProcesses = 1
	if config.has_option('main', 'benchmark') and config.get('main', 'benchmark'):
		benchmark = config.get('main', 'benchmark')
		benchmark = (benchmark == 'True')
	else:
		benchmark = False
	if config.has_option('main', 'ophidiaLogging') and config.get('main', 'ophidiaLogging'):
		ophLog = config.get('main', 'ophidiaLogging')
		ophLog = (ophLog == 'True')
	else:
		ophLog = False

	if config.has_option('ophidia', 'host') and config.get('ophidia', 'host'):
		hostname = config.get('ophidia', 'host')
	else:
		print("Hostname of Ophidia instance not specified in configuration file")
		exit(1)
	if config.has_option('ophidia', 'port') and config.get('ophidia', 'port'):
		port = config.get('ophidia', 'port')
	else:
		port = '11732'

	if token is None and (user is None or password is None):
		if config.has_option('ophidia', 'token') and config.get('ophidia', 'token'):
			token = config.get('ophidia', 'token')
		else:
			if config.has_option('ophidia', 'user') and config.get('ophidia', 'user'):
				user = config.get('ophidia', 'user')
			if config.has_option('ophidia', 'pass') and config.get('ophidia', 'pass'):
				password = config.get('ophidia', 'pass')

	if token is None and (user is None or password is None):
		print("Credentials (user-password or token) for Ophidia instance are not specified")
		exit(1)

	if token is not None:
		user = "__TOKEN__"
		password = token

	if config.has_option('privacy', 'policyFile1') and config.get('privacy', 'policyFile1'):
		policyFile1 = config.get('privacy', 'policyFile1')
	else:
		print("Policy file for Anonymization1 not specified in configuration file")
		exit(1)
	if config.has_option('privacy', 'anonymization1') and config.get('privacy', 'anonymization1'):
		anonymizationBin1 = config.get('privacy', 'anonymization1')
	else:
		print("Anonymization1 executable not specified in configuration file")
		exit(1)
	if config.has_option('privacy', 'policyFile3') and config.get('privacy', 'policyFile3'):
		policyFile3 = config.get('privacy', 'policyFile3')
	else:
		print("Policy file for Anonymization3 not specified in configuration file")
		exit(1)
	if config.has_option('privacy', 'anonymization3') and config.get('privacy', 'anonymization3'):
		anonymizationBin3 = config.get('privacy', 'anonymization3')
	else:
		print("Anonymization3 executable not specified in configuration file")
		exit(1)

	if config.has_option('data', 'inputFolder') and config.get('data', 'inputFolder'):
		inputFolder = config.get('data', 'inputFolder')
	else:
		inputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data")
	if config.has_option('data', 'inputFolder') and config.get('data', 'tmpFolder'):
		tmpFolder = config.get('data', 'tmpFolder')
	else:
		tmpFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_data")
	if config.has_option('data', 'inputFolder') and config.get('data', 'outputFolder'):
		outputFolder = config.get('data', 'outputFolder')
	else:
		outputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_data")
	if config.has_option('data', 'webURL') and config.get('data', 'webURL'):
		webServerUrl = config.get('data', 'webURL')
	else:
		print("URL of web server for data transfer is not defined")
		exit(1)

	print time.strftime('%Y-%m-%d %H:%M:%S')

	if ophLog == True:
		from datetime import datetime
		import logging
		import inspect
		import timeit
		start_time = timeit.default_timer()
		frame = inspect.getframeinfo(inspect.currentframe())
		logging.basicConfig(filename='out.log',level=logging.DEBUG,filemode='a')
		logging.debug('[%s] [%s - %s] ****START LOG****', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno))
		logging.debug('[%s] [%s - %s] singleNcores %d, parallelNcores %d', str(datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), int(singleNcores), int(parallelNcores))

	if input_cubes == True:
		print("No ETL performed") 
		cubePid = cube_list
		print("Ophidia data cube ID: " + str(cubePid[0]))

	else:
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
			anonymFile[i] = privacy.anonymize1File(anonymizationBin1, e, inputFolder, tmpFolder, policyFile1, mode)
			if benchmark == True:
				final_time = timeit.default_timer() - start_time
				print("Time required on file: "+ str(final_time))

		if benchmark == True:
			anonymFile = compss_wait_on(anonymFile)
			global_final_time = timeit.default_timer() - global_start_time
			print("Required time: "+ str(global_final_time))

		#Drop empty cells in array
		anonymFile = [v for v in anonymFile if v is not None]

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("*************************************************\n")

		print("*************************************************")
		print("Starting ETL process")
		print("Running step 1 -> Extraction")

		data = etl.extractPhase(anonymFile, tmpFolder, procType, dq_flag, mode)

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("Running step 2 -> Transformation")

		times , outFile = etl.transformToNetCDF(data, tmpFolder, multiProcesses, procType, mode)

		print time.strftime('%Y-%m-%d %H:%M:%S')

		outFileRef = []
		if distribution in "distributed":
			#Move file to Ophidia instance
			print("Moving files from COMPSs to Ophidia")
			for oF in outFile:
				outName = oF.rsplit("/", 1)[1]
				with tarfile.open(oF + ".tar.gz", "w:gz") as tar:
					tar.add(oF,recursive=False,arcname=outName)
					tar.close()

				#Build Web Server url
				outFileRef.append(webServerUrl + "/" + outName + ".tar.gz")
		else:
			outFileRef = outFile

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("Running step 3 -> Loading")

		if ophLog == True:
			import logging
			import inspect
			import datetime

			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] COMPSs execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

		#Import into Ophidia
		cubePid = etl.loadOphidia(outFileRef, times, singleNcores, user, password, hostname, port, procType, distribution, ophLog)
		print("Ophidia data cube ID: " + str(cubePid[0]))
		if distribution in "distributed":
			for o in outFileRef:
				os.remove(o.replace(webServerUrl + "/",""))

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("End of ETL process")
		print("*************************************************\n")

	#Run computations
	print("*************************************************")
	print("Running statistics computation")

	aggregatedCube, endDate, startDate = dstat.applyFilters(singleNcores, user, password, hostname, port, procType, cubePid, dq_filters, ophLog)
	if procType == "passengerUsage":
		if stats == "all" or stats == "weekly-usage":
			print("Computing: passenger usage stats for each bus line and week in the time range")
			outFile = dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekly-usage", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

			anonymFile = privacy.anonymize3File(anonymizationBin3, outFile, outputFolder, outputFolder, policyFile3, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-usage":
			print("Computing: passenger usage stats for each bus line and month in the time range")
			outFile = dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "monthly-usage", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

			anonymFile = privacy.anonymize3File(anonymizationBin3, outFile, outputFolder, outputFolder, policyFile3, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

	elif procType == "busUsage":
		if stats == "all" or stats == "weekdaysets-peakhours":
			print("Computing: number of passenger stats for each hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-peakhours", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-lines":
			print("Computing: number of passenger stats for each bus line and group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-peakhours":
			print("Computing: number of passenger stats for each hour of weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-peakhours", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-lines":
			print("Computing: number of passenger stats for each bus line and weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-hourly-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-hourly-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-lines":
			print("Computing: number of passenger stats for each bus line and month in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "monthly-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekly-lines":
			print("Computing: number of passenger stats for each bus line and week in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekly-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "daily-lines":
			print("Computing: number of passenger stats for each bus line and day in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "daily-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "hourly-lines", format, outputFolder, procType, mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

	print("*************************************************\n")
	if ophLog == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] TOTAL execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
