import os

def anonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile, mode):

	if mode == 'compss':
		from compss_functions import compssAnonymizeFile
		return compssAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile)
	else:
		from internal_functions import internalAnonymizeFile
		return internalAnonymizeFile(anonymizationBin, inputName, inputFolder, tmpFolder, policyFile)
