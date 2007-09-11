#!/bin/sh

#
# synonymload jobstream wrapper
#
# Usage:
# 	synonymload.sh
#
# Purpose:
#
# History
#
# 09/03/2007	sc
#	- TR8459 
#

cd `dirname $0`

#
# create log file
#

LOG=`pwd`/`basename $0`.log
rm -rf ${LOG}
touch ${LOG}

CONFIG_LOAD=`pwd`/synonymload.config
echo "config: ${CONFIG_LOAD}"
#
# verify & source the synonym load configuration file
#

if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}"
    exit 1
fi

. ${CONFIG_LOAD}

echo ${SYNONYMLOAD}
date | tee ${SYNLOG}
# run synonymload
${SYNONYMLOAD}/synonymload.py  | tee -a ${SYNLOG}
date | tee -a ${SYNLOG}
exit 0

