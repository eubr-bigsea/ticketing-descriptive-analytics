import sys, os, pandas, numpy
from PyOphidia import cube, client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import internal_functions as internal

from pycompss.api.constraint import constraint
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on

@task(anonymizationBin=IN, inputName=IN, inputFolder=IN, tmpFolder=IN, policyFile=IN, returns=str)
def compssAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile):
	#Create tmp folder
	if not os.path.exists(tmpFolder):
	    os.makedirs(tmpFolder)

	return internal.internalAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile)

@task(inputFolder=IN, inputName=IN, delFlag=IN, returns=pandas.DataFrame)
def compssExtractFromFile(inputFolder, inputName, delFlag):
	return internal.internalExtractFromFile(inputFolder, inputName, delFlag)

@task(inputFolder=IN, inputName=IN, returns=pandas.DataFrame)
def compssExtractFromEMFile(inputFolder, inputName):
	return internal.internalExtractFromEMFile(inputFolder, inputName)

@task(sub_x=IN, sub_y=IN, sub_times=IN, x=IN, y=IN, time_val=IN, returns=numpy.ndarray)
def compssTransform(sub_x, sub_y, sub_times, x, y, time_val):
	return internal.internalTransform(sub_x, sub_y, sub_times, x, y, time_val)

@task(sub_x=IN, sub_y=IN, sub_times=IN, sub_m=IN, x=IN, y=IN, time_val=IN, returns=numpy.ndarray)
def compssEMTransform(sub_x, sub_y, sub_times, sub_m, x, y, time_val):
	return internal.internalEMTransform(sub_x, sub_y, sub_times, sub_m, x, y, time_val)

@task(sub_x=IN, sub_y=IN, sub_times=IN, x=IN, y=IN, time_val=IN, returns=numpy.ndarray)
def compssTransformDQ(sub_x, sub_y, sub_times, sub_dq1, sub_dq2, sub_dq3, x, y, time_val):
	return internal.internalTransformDQ(sub_x, sub_y, sub_times, sub_dq1, sub_dq2, sub_dq3, x, y, time_val)

#Functions for Ophidia aggregations
@task(startCube=IN, metric=IN, parallelNcores=IN, user=IN, pwd=IN, host=IN, port=IN, returns=dict)
def compssSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port):
	return internal.internalSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, spatialReduction=IN, user=IN, pwd=IN, host=IN, port=IN, returns=dict)
def compssReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port):
	return internal.internalReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, spatialReduction=IN, user=IN, pwd=IN, host=IN, port=IN, returns=dict)
def compssVerticalAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port):
	return internal.internalVerticalAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, user=IN, pwd=IN, host=IN, port=IN, returns=dict)
def compssTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port):
	return internal.internalTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, user=IN, pwd=IN, host=IN, port=IN, returns=dict)
def compssTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port):
	return internal.internalTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
