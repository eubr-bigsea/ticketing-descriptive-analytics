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

	if token is None and (user is None or password is None):
		print("Credentials (user-password or token) for Ophidia instance are not specified")
		exit(1)

	if token is not None:
		user = "__TOKEN__"
		password = token

	if confFile == "config.ini":
		configFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
	else:
		configFile = confFile

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

	if config.has_option('ophidia', 'host'):
		hostname = config.get('ophidia', 'host')
	else:
		print("Hostname of Ophidia instance not specified in configuration file")
		exit(1)
	if config.has_option('ophidia', 'port'):
		port = config.get('ophidia', 'port')
	else:
		port = '11732'

	if config.has_option('privacy', 'policyFile1'):
		policyFile1 = config.get('privacy', 'policyFile1')
	else:
		print("Policy file for Anonymization1 not specified in configuration file")
		exit(1)
	if config.has_option('privacy', 'anonymization1'):
		anonymizationBin1 = config.get('privacy', 'anonymization1')
	else:
		print("Anonymization1 executable not specified in configuration file")
		exit(1)
	if config.has_option('privacy', 'policyFile3'):
		policyFile3 = config.get('privacy', 'policyFile3')
	else:
		print("Policy file for Anonymization3 not specified in configuration file")
		exit(1)
	if config.has_option('privacy', 'anonymization3'):
		anonymizationBin3 = config.get('privacy', 'anonymization3')
	else:
		print("Anonymization3 executable not specified in configuration file")
		exit(1)

	if config.has_option('data', 'inputFolder'):
		inputFolder = config.get('data', 'inputFolder')
	else:
		inputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input_data")
	if config.has_option('data', 'inputFolder'):
		tmpFolder = config.get('data', 'tmpFolder')
	else:
		tmpFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_data")
	if config.has_option('data', 'inputFolder'):
		outputFolder = config.get('data', 'outputFolder')
	else:
		outputFolder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_data")
	if config.has_option('data', 'webURL'):
		webServerUrl = config.get('data', 'webURL')
	else:
		print("URL of web server for data transfer is not defined")
		exit(1)

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
		anonymFile[i] = privacy.anonymize1File(anonymizationBin1, e, inputFolder, tmpFolder, policyFile1, mode)
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

	outFileRef = ""
	if distribution in "distributed":
		#Move file to Ophidia instance
		print("Moving file from COMPSs to Ophidia")
		outName = outFile.rsplit("/", 1)[1]
		with tarfile.open(outFile + ".tar.gz", "w:gz") as tar:
			tar.add(outFile,recursive=False,arcname=outName)
			tar.close()

		#Build Web Server url
		outFileRef = webServerUrl + "/" + outName + ".tar.gz"
	else:
		outFileRef = outFile

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("Running step 3 -> Loading")

	#Import into Ophidia
	etl.loadOphidia(outFileRef, times, singleNcores, user, password, hostname, port, procType, distribution)	

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("End of ETL process")
	print("*************************************************\n")

	#Run computations
	print("*************************************************")
	print("Running statistics computation")


	if procType == "passengerUsage":
		if stats == "all" or stats == "weekly-usage":
			print("Computing: passenger usage stats for each bus line and week in the time range")
			outFile = dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekly-usage", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

			anonymFile = privacy.anonymize3File(anonymizationBin3, outFile, outputFolder, outputFolder, policyFile3, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

		if stats == "all" or stats == "monthly-usage":
			print("Computing: passenger usage stats for each bus line and month in the time range")
			outFile = dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "monthly-usage", format, outputFolder, procType, mode)
			print time.strftime('%Y-%m-%d %H:%M:%S')

			anonymFile = privacy.anonymize3File(anonymizationBin3, outFile, outputFolder, outputFolder, policyFile3, mode)
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
