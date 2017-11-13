import os, subprocess

def anonymize1File(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile, mode):

	if mode == 'compss':
		from compss_functions import compssAnonymizeFile
		return compssAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile)
	else:
		from internal_functions import internalAnonymizeFile
		return internalAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile)

def anonymize3File(anonymizationBin, inputName, inputFolder, outputFolder, policyFile, mode):

	try:
		inputFile = os.path.join(inputFolder, inputName)
		inFilename, inFileExt = os.path.splitext(inputName)
		outputFile = os.path.join(outputFolder, inFilename + "_anonymized" + inFileExt)
		anonymizationPath = os.path.dirname(anonymizationBin)
	except:
		raise RuntimeError("Error in matching paths")

	if os.path.isfile(inputFile):
		if inFileExt == '.csv':
			print("Anonymizing file (Phase 3): \"" + inputName + "\"")

			try:
				proc = subprocess.check_call(["java -jar " + anonymizationBin + " " + inputFile + " " + policyFile + " " + outputFile + " " + anonymizationPath], stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT, shell=True)
			except subprocess.CalledProcessError:
				print("Unable to run anonymization tool")

			os.remove(inputFile)
			return outputFile

	return None
