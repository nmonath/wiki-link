#!/bin/bash

LOG_DIR=logs/`basename $1`
OUTPUT_DIR=pages/`basename $1`

echo $LOG_DIR
echo $OUTPUT_DIR

mkdir -p $LOG_DIR
mkdir -p $OUTPUT_DIR

if [ ! -e "$LOG_DIR/complete" ]
then
	LINE_NUM=0
	for line in `cat $1`
	do
	  wget --timeout=20 --tries=2 -o $LOG_DIR/$LINE_NUM -O $OUTPUT_DIR/$LINE_NUM $line
	  ((LINE_NUM++))
	done

	touch $LOG_DIR/complete
else
	echo "Skipping complete chunk:  " `basename $1`
fi
