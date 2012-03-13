#!/bin/bash

sbt "run-main edu.umass.cs.iesl.wikilink.retrieve.Retrieve @file=/iesl/canvas/sameer/dat/wiki-link/data-00000-of-00001 @output=output @resume=true @workers=100"
