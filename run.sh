#!/bin/bash

mvn scala:run \ 
  -DmainClass=edu.umass.cs.iesl.wikilink.retrieve.Retrieve \
  -DaddArgs="@file=/iesl/canvas/sameer/dat/wiki-link/data-00000-of-00001|@output=output|@resume=true|@workers=100"
