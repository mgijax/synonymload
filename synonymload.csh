#!/bin/csh -f

#
# Wrapper script to create & load new marker synonyms
#
# Usage:  synonymload.csh
#

# DB schema directory; its Configuration file will set up all you need
setenv SCHEMADIR $1
setenv INPUTFILE	$2
setenv MODE		$3

setenv SYNLOAD		/usr/local/mgi/dataload/synonymload
setenv SYNLOAD		/home/lec/loads/synonymload

source ${SCHEMADIR}/Configuration
setenv LOG	$0.log

date >& $LOG

${SYNLOAD}/synonymload.py -S${DBSERVER} -D${DBNAME} -U${DBUSER} -P${DBPASSWORDFILE} -M${MODE} -I${INPUTFILE} >>& $LOG

date >>& $LOG

