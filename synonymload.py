#!/usr/local/bin/python

#
# Purpose:
#
#	To load new records into MGI Synonym
#		. MGI_Synonym
#
# Assumes:
#
#	That no one else is adding Nomen records to the database.
#
# Side Effects:
#
#	None
#
# Input:
#
#	A tab-delimited file in the format:
#		field 1: Object Accession ID
#		field 2: Synonym
#		field 3: Synonym Type (from MGI_Synonym_Type)
#
#	processing modes:
#		load - load the data
#
#		preview - perform all record verifications, create all files
#		          but do not execute bcp or make any changes to the 
#			  database. 
#
#		reload - delete existing synonyms by 'createdBy' and object type
#	                 then process records and do bcp
#
# Output:
#
#       1 BCP file:
#
#       MGI_Synonym.bcp
#
#	Diagnostics file of all input parameters and SQL commands
#	Error file
#
# Processing:
#
#	1. Verify Mode.
#		if mode = load:  process records, do bcp
#		if mode = preview:  set "bcpon" to False
#		if mode = reload: first delete existing synonyms by 'createdBy'
#			  then process records and do bcp
#
#	2. Load Synonym Types into dictionary for quicker lookup.
#
#	For each line in the input file:
#
#	1.  Verify the Accession ID is valid.
#	    If the verification fails, report the error and skip the record.
#
#	2.  Verify the Synonym Type is valid.
#	    If the verification fails, report the error and skip the record.
#
#	3.  Verify the J: is valid.
#	    If the verification fails, report the error and skip the record.
#	    If the verification succeeds, store the Jnum/Key pair in a 
#	    dictionary for future reference.
#
#	4.  Verify the Created By is valid
#	    If the verification fails, report the error and skip the record.
#
#	5.  Create the Synonym record.
#
# History:
#
# 02/08/2006	lec
#	- converted from MRK_Other synonym load; primarily for JRS cutover
#
# 09/04/2007	sc
#	- updated to be less JRS-centric and configurable

import sys
import os
import string
import db
import mgi_utils
import loadlib

#globals

#
# from configuration file
#
user = os.environ['PG_DBUSER']
passwordFileName = os.environ['PG_1LINE_PASSFILE']
mode = os.environ['LOAD_MODE']
mgiType = os.environ['OBJECT_TYPE']
inputFileName = os.environ['INPUTFILE']
createdBy = os.environ['CREATEDBY']
jnum = os.environ['JNUM']
logDir = os.environ['LOGDIR']
outputDir = os.environ['OUTPUTDIR']

bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh '

bcpon = 1		# can the bcp files be bcp-ed into the database?  

inputFile = ''		# file descriptor
diagFile = ''		# file descriptor
errorFile = ''		# file descriptor
synFile = ''		# file descriptor

diagFileName = ''	# file name
errorFileName = ''	# file name
synFileName = ''	# file name

mgiTypeKey = 0		# ACC_MGIType._MGIType_key
referenceKey = 0	# MGI_Synonym._Refs_key
synKey = 0		# MGI_Synonym._Synonym_key
createdByKey = 0        # MGI_Synonym._CreatedBy_key

synTypeDict = {}	# dictionary of synonym types for quick lookup
synDict = {}		# dictionary of

loaddate = loadlib.loaddate

def exit(status, message = None):
	# requires: status, the numeric exit status (integer)
	#           message (string)
	#
	# effects:
	# Print message to stderr and exits
	#
	# returns:
	#
 
	if message is not None:
		sys.stderr.write('\n' + str(message) + '\n')
 
	try:
		inputFile.close()
		diagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
		errorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
		diagFile.close()
		errorFile.close()
	except:
		pass

	db.useOneConnection()
	sys.exit(status)
 
def init():
	# requires: 
	#
	# effects: 
	# 1. Processes command line options
	# 2. Initializes local DBMS parameters
	# 3. Initializes global file descriptors/file names
	# 4. Initializes global keys
	#
	# returns:
	#
 
	global diagFileName, errorFileName, synFileName
	global inputFile, diagFile, errorFile, synFile
	global mgiTypeKey, createdByKey, referenceKey

	db.useOneConnection(1)
        db.set_sqlUser(user)
        db.set_sqlPasswordFromFile(passwordFileName)
 
	head, tail = os.path.split(inputFileName) 
	diagFileName = logDir + '/' + tail + '.diagnostics'
	errorFileName = logDir + '/' + tail + '.error'
	synFileName = 'MGI_Synonym.bcp'

	print inputFileName
	print logDir

	try:
		inputFile = open(inputFileName, 'r')
	except:
		exit(1, 'Could not open file %s\n' % inputFileName)
		
	try:
		diagFile = open(diagFileName, 'w')
	except:
		exit(1, 'Could not open file %s\n' % diagFileName)
		
	try:
		errorFile = open(errorFileName, 'w')
	except:
		exit(1, 'Could not open file %s\n' % errorFileName)
		
	try:
		synFile = open(outputDir + '/' + synFileName, 'w')
	except:
		exit(1, 'Could not open file %s\n' % synFileName)
		
	# Log all SQL
	db.set_sqlLogFunction(db.sqlLogAll)

	diagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
	diagFile.write('Server: %s\n' % (db.get_sqlServer()))
	diagFile.write('Database: %s\n' % (db.get_sqlDatabase()))
	diagFile.write('Object Type: %s\n' % (mgiType))
	diagFile.write('Input File: %s\n' % (inputFileName))

	errorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

	mgiTypeKey = loadlib.verifyMGIType(mgiType, 0, errorFile)
	createdByKey = loadlib.verifyUser(createdBy, 0, errorFile)

        # if reference is J:0, then no reference is given
	if jnum == 'J:0':
		referenceKey = ''
	else:
		referenceKey = loadlib.verifyReference(jnum, 0, errorFile)

        # exit if we can't resolve mgiType, createdBy or jnum
	if mgiTypeKey == 0 or \
		createdByKey == 0 or \
		referenceKey == 0:
	    exit(1)

        if mode == 'reload':
		print 'mode is: %s, deleting synonyms' % mode
		sys.stdout.flush()
        	db.sql('delete from MGI_Synonym ' + \
			'where _MGIType_key = %d ' % (mgiTypeKey) + \
			'and _CreatedBy_key = %d ' % (createdByKey), None)

def verifyMode():
	# requires:
	#
	# effects:
	#	Verifies the processing mode is valid.  If it is not valid,
	#	the program is aborted.
	#	Sets globals based on processing mode.
	#	Deletes data based on processing mode.
	#
	# returns:
	#	nothing
	#

	global bcpon
	if mode == 'preview':
		bcpon = 0
	elif  mode != 'load' and mode != 'reload':
		exit(1, 'Invalid Processing Mode:  %s\n' % (mode))

def verifySynonymType(synType, lineNum):
	# requires:
	#	synType - the Synonym Type
	#	lineNum - the line number of the record from the input file
	#
	# effects:
	#	verifies that:
	#		the Synonym Type exists 
	#	writes to the error file if the Synonym Type is invalid
	#
	# returns:
	#	0 if the Synonym Type is invalid
	#	Synonym Type Key if the Synonym Type is valid
	#

	synTypeKey = 0

	if synTypeDict.has_key(synType):
		synTypeKey = synTypeDict[synType]
	else:
		errorFile.write('Invalid Synonym Type (%d) %s\n' % (lineNum, synType))
		synTypeKey = 0

	return(synTypeKey)

def setPrimaryKeys():
	# requires:
	#
	# effects:
	#	Sets the global primary keys values needed for the load
	#
	# returns:
	#	nothing
	#

	global synKey

        results = db.sql(' select nextval(mgi_synonym_seq') as maxKey ', 'auto')
        synKey = results[0]['maxKey']

def loadDictionaries():
	# requires:
	#
	# effects:
	#	loads global dictionaries for quicker lookup
	#
	# returns:
	#	nothing

	global synTypeDict, synDict
	
	# create synonym type lookup
	results = db.sql('select _SynonymType_key, synonymType from MGI_SynonymType ' + \
		'where _MGIType_key = %s' % (mgiTypeKey), 'auto')
	for r in results:
		synTypeDict[r['synonymType']] = r['_SynonymType_key']

	# create existing synonym lookup for all MGI accessioned objects
	results = db.sql('''select a.accid as mgiID, s.synonym 
	    from MRK_Marker m, ACC_Accession a, MGI_Synonym s
	    where m._Organism_key = 1 
	    and m._Marker_key = a._Object_key 
	    and a._LogicalDB_key = 1 
	    and a._MGIType_key = 2 
	    and a.prefixPart = 'MGI:' 
	    and a.preferred = 1 
	    and a._MGIType_key = s._MGIType_key 
	    and a._Object_key = s._Object_key
	    ''', 'auto')
	for r in results:
	    mgiID = r['mgiID']
	    synonym = r['synonym']
	    if mgiID not in synDict.keys():
	        synDict[mgiID] = [synonym]
	    else:
		synDict[mgiID].append(synonym)

def processFile():
	# requires:
	#
	# effects:
	#	Reads input file
	#	Verifies and Processes each line in the input file
	#
	# returns:
	#	nothing
	#

	global synKey
        mgiIdsWithSynonyms = synDict.keys()

	lineNum = 0

	# For each line in the input file

	for line in inputFile.readlines():

		error = 0
		lineNum = lineNum + 1

		# Split the line into tokens
		tokens = string.split(line[:-1], '\t')

		try:
			accID = tokens[0]
			synonym = tokens[1]
			synType = tokens[2]
		except:
			exit(1, 'Invalid Line (%d): %s\n' % (lineNum, line))

		objectKey = loadlib.verifyObject(accID, mgiTypeKey, None, lineNum, errorFile)

		if accID in mgiIdsWithSynonyms:
		    if synonym in synDict[accID]:
			errorFile.write('Duplicate synonym: %s for %s\n' % (synonym, accID))
			continue

		synTypeKey = verifySynonymType(synType, lineNum)

		if len(synonym) == 0:
		    errorFile.write('Invalid Synonym:Empty (%d) %s\n' % (lineNum, synonym))

		if objectKey == 0 or \
			synTypeKey == 0 or \
			len(synonym) == 0:

			# set error flag to true
			error = 1

		# if errors, continue to next record
		if error:
			continue

		# if no errors, process

		synFile.write('%d|%d|%d|%d|%s|%s|%s|%s|%s|%s\n' \
			% (synKey, objectKey, mgiTypeKey, synTypeKey, referenceKey, synonym, createdByKey, createdByKey, loaddate, loaddate))
		synKey = synKey + 1

#	end of "for line in inputFile.readlines():"

def bcpFiles():
	# requires:
	#
	# effects:
	#	BCPs the data into the database
	#
	# returns:
	#	nothing
	#

	synFile.close()
        if not bcpon:
	    print 'Skipping BCP. Mode: %s' % mode
	    sys.stdout.flush()
            return
	print 'Executing BCP'
	sys.stdout.flush()
	db.commit()
	bcp1 = bcpCommand % ('MGI_Synonym', synFileName)
	diagFile.write('%s\n' % bcp1)
	os.system(bcp1)
	db.commit()

        db.sql(''' select setval('mgi_synonym_seq', (select max(_Synonym_key) from MGI_Synonym)) ''', None)
        db.commit()

#
# Main
#

print 'Initializing'
sys.stdout.flush()
init()

print 'Verifying Load Mode'
sys.stdout.flush()
verifyMode()
setPrimaryKeys()

print 'Creating Lookups'
sys.stdout.flush()
loadDictionaries()

print 'Processing Input File'
sys.stdout.flush()
processFile()
bcpFiles()

exit(0)

