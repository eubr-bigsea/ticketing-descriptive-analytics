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

@task(inputFolder=IN, inputName=IN, returns=pandas.DataFrame)
def compssExtractFromFile(inputFolder, inputName):
	return internal.internalExtractFromFile(inputFolder, inputName)

@task(sub_times=IN, time_val=IN, returns=numpy.ndarray)
def compssTransform(sub_times, time_val):
	return internal.internalTransform(sub_times, time_val)

#Functions for Ophidia aggregations
@task(startCube=IN, metric=IN, parallelNcores=IN, user=IN, pwd=IN, host=IN, port=IN, returns=cube.Cube)
def compssSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port):
	return internal.internalSimpleAggregation(startCube, metric, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, spatialReduction=IN, user=IN, pwd=IN, host=IN, port=IN, returns=cube.Cube)
def compssReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port):
	return internal.internalReducedAggregation(startCube, metric, spatialReduction, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, user=IN, pwd=IN, host=IN, port=IN, returns=cube.Cube)
def compssTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port):
	return internal.internalTotalAggregation(startCube, metric, parallelNcores, user, pwd, host, port)

@task(startCube=IN, metric=IN, parallelNcores=IN, user=IN, pwd=IN, host=IN, port=IN, returns=cube.Cube)
def compssTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port):
	return internal.internalTotalHourlyAggregation(startCube, metric, parallelNcores, user, pwd, host, port)
