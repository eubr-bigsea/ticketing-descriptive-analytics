import os, sys, time, shutil
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import common_functions as common
import descriptive_stats as dstat
import ticketing_etl as etl
import argparse
import time

from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Compute Descriptive Analytics with COMPSs and Ophidia on ticketing data')
	parser.add_argument('-p','--processing', default="all", help='Type of processing available', choices=["all", "weekdaysets-peakhours", "weekdaysets-lines", "weekdays-peakhours", "weekdays-lines", "weekdays-hourly-lines", "weekdaysets-hourly-lines", "monthly-lines", "weekly-lines", "daily-lines", "hourly-lines"])
	parser.add_argument('-f','--format', default="csv", help='Type of output format', choices=['json','csv'])
	parser.add_argument('-d','--dir', default="$PWD", help='Base path of input_data, tmp_data and output_data')
	args = parser.parse_args()

	processing = args.processing
	format = args.format
	dataDir = args.dir

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

	anonymFile = [0 for m in range(0,len(os.listdir(inputFolder)))]
	for i, e in enumerate(sorted(os.listdir(inputFolder))):
		anonymFile[i] = common.anonymizeFile(anonymizationBin, e, inputFolder, tmpFolder, policyFile)

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("*************************************************\n")

	print("*************************************************")
	print("Starting ETL process")
	print("Running step 1 -> Extraction")

	import pandas
	#Loop on input files
	data = [0 for m in range(0, len(anonymFile))]
	for i, e in enumerate(anonymFile):
		data[i] = etl.extractFromFiles(tmpFolder, e)
	data = compss_wait_on(data)

	data = pandas.concat([d for d in data], ignore_index=True)
	data.sort_values(['CODLINHA', 'NOMELINHA', 'CODVEICULO', 'DATAUTILIZACAO'], ascending=[True, True, True, True], inplace=True)

	line = data.values[:,0].flatten('F')
	vehicle = data.values[:,1].flatten('F')
	date = data.values[:,2].flatten('F')

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("Running step 2 -> Transformation")

	times , outFile = etl.transformToNetCDF(line, vehicle, date, tmpFolder, multiProcesses)

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("Running step 3 -> Loading")

	#Import into Ophidia
	etl.loadOphidia(outFile, times, singleNcores, user, password, hostname, port)

	print time.strftime('%Y-%m-%d %H:%M:%S')
	print("End of ETL process")
	print("*************************************************\n")

	#Run processing
	print("*************************************************")
	print("Running statistics computation")

	if processing == "all" or processing == "weekdaysets-peakhours":
		print("Computing: number of passenger stats for each hour of group of weekdays")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-peakhours", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "weekdaysets-lines":
		print("Computing: number of passenger stats for each bus line and group of weekdays")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "weekdays-peakhours":
		print("Computing: number of passenger stats for each hour of weekday")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-peakhours", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "weekdays-lines":
		print("Computing: number of passenger stats for each bus line and weekday")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "weekdays-hourly-lines":
		print("Computing: number of passenger stats for each bus line and hour of weekdays")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdays-hourly-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "weekdaysets-hourly-lines":
		print("Computing: number of passenger stats for each bus line and hour of group of weekdays")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekdaysets-hourly-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "monthly-lines":
		print("Computing: number of passenger stats for each bus line and month in the time range")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "monthly-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "weekly-lines":
		print("Computing: number of passenger stats for each bus line and week in the time range")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "weekly-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "daily-lines":
		print("Computing: number of passenger stats for each bus line and day in the time range")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "daily-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	if processing == "all" or processing == "hourly-lines":
		print("Computing: number of passenger stats for each bus line and hour in the time range")
		dstat.computeTicketingStat(parallelNcores, singleNcores, user, password, hostname, port, "hourly-lines", format, outputFolder)
		print time.strftime('%Y-%m-%d %H:%M:%S')

	print("*************************************************\n")

