#!/bin/bash

PAGES_DIR=""

if [ $# -ne 1 ]
then
  echo "Usage: ./`basename $0` retrieved-pages-dir/"
  exit 1
else
  PAGES_DIR=$1
fi

find $PAGES_DIR/ -name "*" -type f | parallel --progress -j0 file -b -i > filetypes.dat
cat filetypes.dat | sort | uniq -c | sort > filetype_distribution.out
