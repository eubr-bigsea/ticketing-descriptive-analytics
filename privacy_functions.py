import os, shutil, subprocess
import common_functions as common

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
			newFile = common.jsonLine2json(inputFile)

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
