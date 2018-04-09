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
	parser.add_argument('-a','--aggregated', default="no", choices=['no','yes'], help='If the cube produced must be pre-aggregated or not')
	subparsers = parser.add_subparsers(dest="type", help='Type of index to evaluate')

	parserA = subparsers.add_parser('bus-usage',description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserA.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "weekdaysets-peakhours", "weekdaysets-lines", "weekdays-peakhours", "weekdays-lines", "weekdays-hourly-lines", "weekdaysets-hourly-lines", "monthly-lines", "weekly-lines", "daily-lines", "hourly-lines"])

	parserB = subparsers.add_parser('passenger-usage', description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserB.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "monthly-usage", "weekly-usage"])

	parserC = subparsers.add_parser('bus-stops',description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parserC.add_argument('-s','--stats', default="all", help='Type of stats available', choices=["all", "weekdaysets-stops", "weekdays-stops", "weekdays-hourly-stops", "weekdaysets-hourly-stops", "monthly-stops", "weekly-stops", "daily-stops", "hourly-stops"])

	parserD = subparsers.add_parser('all',description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')

	args = parser.parse_args()

	if args.type == 'passenger-usage':
		procType = "passengerUsage"
	elif args.type == 'bus-usage':
		procType = "busUsage"
	elif args.type == 'bus-stops':
		procType = "busStops"
	elif args.type == 'all':
		procType = "all"

	if procType != "all":
		stats = args.stats
	else:
		stats = "all"

	format = args.format
	confFile = args.conf
	mode = args.mode
	distribution = args.distribution
	aggregated = args.aggregated

	if aggregated == "yes":
		aggregated = True
	else:
		aggregated = False

	user = args.username
	password = args.password
	token = args.token

	#Check data quality filters
	dq_filters = args.dq_filters
	if args.type == 'bus-stops':
		dq_filters = "None"

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
			if not any(dq_filters):
				dq_flag = False

	#Match datacube PIDs
	input_cubes = args.input_cube
	if input_cubes == "None" or procType == "all":
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

	if config.has_option('data', 'inputFolderDQ') and config.get('data', 'inputFolderDQ'):
		inputFolderDQ = config.get('data', 'inputFolderDQ')
	else:
		inputFolderDQ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data_dq")
	if config.has_option('data', 'inputFolderEM') and config.get('data', 'inputFolderEM'):
		inputFolderEM = config.get('data', 'inputFolderEM')
	else:
		inputFolderEM = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data_em")
	if config.has_option('data', 'tmpFolder') and config.get('data', 'tmpFolder'):
		tmpFolder = config.get('data', 'tmpFolder')
	else:
		tmpFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_data")
	if config.has_option('data', 'outputFolder') and config.get('data', 'outputFolder'):
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

		anonymFile = [None, None]
		if procType != "busStops" or procType == "all":

			fileNum, fileList = common.getFiles(inputFolderDQ)
			anonymFile[0] = [None for m in range(0,fileNum)]
			for i, e in enumerate(sorted(fileList)):
				if benchmark == True:
					start_time = timeit.default_timer()
				anonymFile[0][i] = privacy.anonymize1File(anonymizationBin1, e, tmpFolder, policyFile1, mode)
				if benchmark == True:
					final_time = timeit.default_timer() - start_time
					print("Time required on file: "+ str(final_time))

			if benchmark == True:
				from pycompss.api.constraint import constraint
				from pycompss.api.task import task
				from pycompss.api.parameter import *
				from pycompss.api.api import compss_wait_on
				anonymFile[0] = compss_wait_on(anonymFile[0])
				global_final_time = timeit.default_timer() - global_start_time
				print("Required time: "+ str(global_final_time))

			#Drop empty cells in array
			anonymFile[0] = [v for v in anonymFile[0] if v is not None]

		if procType == "busStops" or procType == "all":

			fileNum, fileList = common.getFiles(inputFolderEM)
			anonymFile[1] = [None for m in range(0,fileNum)]
			for i, e in enumerate(sorted(fileList)):
				if os.path.isfile(e) and common.checkFormat(e, "csv"):
					anonymFile[1][i] = e

			#Drop empty cells in array
			anonymFile[1] = [v for v in anonymFile[1] if v is not None]

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("*************************************************\n")

		print("*************************************************")
		print("Starting ETL process")
		print("Running step 1 -> Extraction")

		data = [None, None, None]
		if procType == "passengerUsage" or procType == "all":
			data[0] = etl.extractPhase(anonymFile[0], tmpFolder, "passengerUsage", dq_flag, mode, False, aggregated)
		if procType == "busUsage" or procType == "all":
			data[1] = etl.extractPhase(anonymFile[0], tmpFolder, "busUsage", dq_flag, mode, True, False)
		if procType == "busStops" or procType == "all":
			data[2] = etl.extractPhase(anonymFile[1], tmpFolder, "busStops", False, mode, False, False)

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("Running step 2 -> Transformation")

		times = [None, None, None]
		outFile = [None, None, None]
		if procType == "passengerUsage" or procType == "all":
			times[0], outFile[0] = etl.transformToNetCDF(data[0], tmpFolder, multiProcesses, "passengerUsage", mode, aggregated)
		if procType == "busUsage" or procType == "all":
			times[1], outFile[1] = etl.transformToNetCDF(data[1], tmpFolder, multiProcesses, "busUsage", mode, False)
		if procType == "busStops" or procType == "all":
			times[2], outFile[2] = etl.transformToNetCDF(data[2], tmpFolder, multiProcesses, "busStops", mode, False)

		print time.strftime('%Y-%m-%d %H:%M:%S')

		outFileRef = [None, None, None]
		if distribution in "distributed":
			#Move file to Ophidia instance
			print("Moving files from COMPSs to Ophidia")
			for f in range(len(outFile)):
				if outFile[f] is not None:
					outFileRef[f] = []
					for oF in outFile[f]:
						outName = oF.rsplit("/", 1)[1]
						with tarfile.open(oF + ".tar.gz", "w:gz") as tar:
							tar.add(oF,recursive=False,arcname=outName)
							tar.close()

						#Build Web Server url
						outFileRef[f].append(webServerUrl + "/" + outName + ".tar.gz")
			print time.strftime('%Y-%m-%d %H:%M:%S')
		else:
			outFileRef = outFile

		print("Running step 3 -> Loading")

		if ophLog == True:
			import logging
			import inspect
			import datetime

			end_time = timeit.default_timer() - start_time
			logging.debug('[%s] [%s - %s] COMPSs execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))

		#Import into Ophidia
		cubePid = [None, None, None]
		if procType == "passengerUsage" or procType == "all":
			cubePid[0] = etl.loadOphidia(outFileRef[0], times[0], singleNcores, user, password, hostname, port, "passengerUsage", distribution, ophLog, aggregated)
		if procType == "busUsage" or procType == "all":
			cubePid[1] = etl.loadOphidia(outFileRef[1], times[1], singleNcores, user, password, hostname, port, "busUsage", distribution, ophLog, False)
		if procType == "busStops" or procType == "all":
			cubePid[2] = etl.loadOphidia(outFileRef[2], times[2], singleNcores, user, password, hostname, port, "busStops", distribution, ophLog, False)

		print("Ophidia data cube ID: " + str(cubePid[0]))
		if distribution in "distributed":
			for f in range(len(outFileRef)):
				if outFileRef[f] is not None:
					for o in outFileRef[f]:
						os.remove(o.replace(webServerUrl + "/",""))

		print time.strftime('%Y-%m-%d %H:%M:%S')
		print("End of ETL process")
		print("*************************************************\n")

	#Run computations
	print("*************************************************")
	print("Running statistics computation")

	if procType == "passengerUsage" or procType == "all":
		aggregatedCube, endDate, startDate = dstat.applyFilters(singleNcores, user, password, hostname, port, "passengerUsage", cubePid[0], dq_filters, ophLog, aggregated)
		if stats == "all" or stats == "weekly-usage":
			print("Computing: passenger usage stats for each bus line and week in the time range")
			outFile = dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekly-usage", format, outputFolder, "passengerUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

			anonymFile = privacy.anonymize3File(anonymizationBin3, outFile, outputFolder, outputFolder, policyFile3, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-usage":
			print("Computing: passenger usage stats for each bus line and month in the time range")
			outFile = dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "monthly-usage", format, outputFolder, "passengerUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

			anonymFile = privacy.anonymize3File(anonymizationBin3, outFile, outputFolder, outputFolder, policyFile3, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		dstat.removeTempCubes(singleNcores, user, password, hostname, port, ophLog)
	if procType == "busUsage" or procType == "all":
		aggregatedCube, endDate, startDate = dstat.applyFilters(singleNcores, user, password, hostname, port, "busUsage", cubePid[1], dq_filters, ophLog, False)
		if stats == "all" or stats == "weekdaysets-peakhours":
			print("Computing: number of passenger stats for each hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-peakhours", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-lines":
			print("Computing: number of passenger stats for each bus line and group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-peakhours":
			print("Computing: number of passenger stats for each hour of weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-peakhours", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-lines":
			print("Computing: number of passenger stats for each bus line and weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-hourly-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-hourly-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-lines":
			print("Computing: number of passenger stats for each bus line and month in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "monthly-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekly-lines":
			print("Computing: number of passenger stats for each bus line and week in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekly-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "daily-lines":
			print("Computing: number of passenger stats for each bus line and day in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "daily-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "hourly-lines":
			print("Computing: number of passenger stats for each bus line and hour in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "hourly-lines", format, outputFolder, "busUsage", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		dstat.removeTempCubes(singleNcores, user, password, hostname, port, ophLog)
	if procType == "busStops" or procType == "all":
		aggregatedCube, endDate, startDate = dstat.applyFilters(singleNcores, user, password, hostname, port, "busStops", cubePid[2], [None, None, None], ophLog, False)
		if stats == "all" or stats == "weekdaysets-stops":
			print("Computing: number of passenger stats for each bus stop and group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-stops":
			print("Computing: number of passenger stats for each bus stop and weekday")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdays-hourly-stops":
			print("Computing: number of passenger stats for each bus stop and hour of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdays-hourly-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekdaysets-hourly-stops":
			print("Computing: number of passenger stats for each bus stop and hour of group of weekdays")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekdaysets-hourly-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-stops":
			print("Computing: number of passenger stats for each bus stop and month in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "monthly-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "weekly-stops":
			print("Computing: number of passenger stats for each bus stops and week in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "weekly-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "daily-stops":
			print("Computing: number of passenger stats for each bus stop and day in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "daily-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "hourly-stops":
			print("Computing: number of passenger stats for each bus stop and hour in the time range")
			dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, aggregatedCube, endDate, startDate, "hourly-stops", format, outputFolder, "busStops", mode, ophLog)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		dstat.removeTempCubes(singleNcores, user, password, hostname, port, ophLog)

	print("*************************************************\n")
	if ophLog == True:
		end_time = timeit.default_timer() - start_time
		logging.debug('[%s] [%s - %s] TOTAL execution time: %s [s]', str(datetime.datetime.now()), str(os.path.basename(frame.filename)), str(frame.lineno), str(end_time))
