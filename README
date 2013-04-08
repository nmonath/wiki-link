# Introduction

This software package contains code that computes the basic statistics on the Google WikiLinks dataset, and
downloads & process webpages to construct a version of the dataset that contains contexts around each mention.

*Note:* This file assumes knowledge of the Wikilinks dataset, see: http://iesl.cs.umass.edu/data/wiki-links

# Analysis of Google Data

This package contains utilities to work with original Google dataset. This includes:

## Iterators (over mentions and webpages)

## Misc Clusterings: including "gold" clustering, "string match" clusterings, etc. (see Tech Report).

## Coreference Metrics: measuring the quality of the above (or any) predicted clustering

# Creating the Dataset with the context

The dataset with context is produced in several stages.  At the moment this includes: retrieval and processing.

## Retrieval: Download the webpages that are referred to in the Google Dataset

  To retrieve the webpages:

    > cd retrieve/; ./retrieve.sh /path/to/original/wiki-link-data

  This will result in several new files in retrieve/:
    - wiki-link-urls.dat -- a file consisting of one URL per line which was used by retrieve.sh
    - logs/ -- the output of wget for each downloaded file
    - pages/ -- the files which were downloaded
      - 000000/
        - 0
        - 1
        - 2
        - ...
      - 000001/
        - ...
      - ....

## Processing: Extract HTML structure and context from the downloaded webpages, and store as thrift files.

  To process the downloaded files:

    > cd process/; mvn scala:run

# Using the Dataset with the context



