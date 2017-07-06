
RDF Delta provides a system for recording and publishing changes to RDF
Datasets. It is built around idea of change logs:

* _RDF Patch_ -  a format for recording changes to an RDF Dataset
* _RDF Patch Log_ - organise patches in to a log of changes 
to an RDF Dataset with HTTP access. 

RDF Patch Logs can be used for:

* Replicated datasets - 2 or more copies of a single dataset for high
availability of the data.
* Incremental backup of a dataset.
* Recording changes 
* Generate alerts based on changes, either to the dataset as a whole or
specific resources within the dataset.

RDF Delta provides a system for keeping copies of an RDF Dataset
up-to-date using the RDF Patch Log as a journal of changes to be applied.

Website: https://afs.github.io/rdf-delta

[![Build Status](https://api.travis-ci.org/afs/rdf-delta.svg)](https://travis-ci.org/afs/rdf-delta)
