# RDF Delta

RDF Delta is a system for recording and publishing changes to RDF
Datasets. There are three parts:

* _RDF Patch_ -  a format for recording changes to an RDF Dataset
* _RDF Patch Logs_ - organise patches in to a log of changes 
to an RDF Dataset with HTTP access. 
* _RDF Patch Logs Server_ - a server for RDF Patch Logs to support
replicated datasets.

RDF Patch Logs can be used for:

* Replicated datasets - 2 or more copies of a single dataset for high
availability of the data.
* Incremental backup of a dataset.
* Recording changes 
* Generate alerts based on changes, either to the dataset as a whole or
specific resources within the dataset.

See "[Delta](delta.md)" for a overview of the Delta system for
distributing changes to RDF datasets. 

## Documentation

* "[Delta](delta.md)" for a overview of the Delta system
* "[Introduction to RDF Patch](rdf-patch-intro.md)"
* "[RDF Patch](rdf-patch.md)" for a specification
* "[Delta Procotol](delta-protocol.md)" for details of protocol of a delta server.
* "[Delta API](delta-api.md)" for the application API (java).

## Code

[https://github.com/afs/rdf-delta](https://github.com/afs/rdf-delta)

[![Build Status](https://api.travis-ci.org/afs/rdf-delta.svg)](https://travis-ci.org/afs/rdf-delta)
