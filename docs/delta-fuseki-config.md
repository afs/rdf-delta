---
layout: doc
title:  Fuseki configuration for High Availability
---


This is the file Fuseki configuration file used in the tutorial, with additional
comments.

```
PREFIX :        <#>
PREFIX fuseki:  <http://jena.apache.org/fuseki#>
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ja:      <http://jena.hpl.hp.com/2005/11/Assembler#>
PREFIX delta:   <http://jena.apache.org/rdf-delta#>

[] rdf:type fuseki:Server .

## A Fuseki service offering all SPARQL protocols.
<#service1> rdf:type fuseki:Service ;
    fuseki:name                        "ds" ;
    fuseki:serviceQuery                "query" ;
    fuseki:serviceUpdate               "update" ;
    fuseki:serviceUpload               "upload" ;
    fuseki:serviceReadWriteGraphStore  "data" ;     
    fuseki:serviceReadGraphStore       "get" ;
    fuseki:dataset                     <#dataset> ;
    .

## The dataset is an in-memory 

## Persistent versions do not need to reread all the data at startup.
## The RDF Delta client library records the latest version of the
## client-side persistent copy so on startup it only needs to fetch
## later patches.

<#dataset> rdf:type delta:DeltaDataset ;

    ## The patch log server
    delta:changes  "http://localhost:1066/" ;
    
    ## The log name for this dataset.
    delta:patchlog "ABC";

    ## The type of storage.
    delta:storage "mem";
    .
```

For a persistent dataset:
```
<#dataset> rdf:type delta:DeltaDataset ;

    ## The patch log server
    delta:changes  "http://localhost:1068/" ;
    
    ## The log name for this dataset.
    delta:patchlog "ABC";

    ## The client library work area.
    ## It can not be shared between servers.

    delta:zone "Zone1";

    ## The type of storage.
    ## Persistent options include "tdb" and "tdb2".
    delta:storage "tdb";
    .
```

For a configuration where the Fuseki serverfails over to another patch
log server:
```
<#dataset> rdf:type delta:DeltaDataset ;

    ## The patch log server
    delta:changes  ("http://host1:port1/" "http://host2:port2/" "http://host3:port3/" );
    ...
```
