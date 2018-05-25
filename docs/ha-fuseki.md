---
layout: doc
title: High Availablity Apache Jena Fuseki
nav_text: HA Fuseki
section: 5
---
This section describes a packaging of RDF Delta with the
[Apache Jena Fuseki](https://jena.apache.org/documentation/fuseki2/index.html)
triple store to provide a high availability solution.

* [Obtain the Software](#software)
* [Demonstration](#demonstration)
* [Fuseki Configuration](#fuseki-config)

# Obtain the Software

To build a High Availablity Apache Jena Fuseki installation, you need a
copy of Fuseki with the RDF Delta client code, and an RDF Delta Patch Log
Server.

These are available in a single package from:

[central.maven.org::rdf-delta-dist](http://central.maven.org/maven2/org/seaborne/org/seaborne/rdf-delta/rdf-delta-dist/)

Download the latse version and unpack the zip file: there will be a
directory `rdf-delta-VER`.

# Demonstration

For demonstration/experimentation purposes, this "getting started" guide
runs all the servers on the local machine.  In a production enviroment,
the different servers should be run on separate machines each on
separate hardware.

## Run the servers

It is convenient to run each of these comands on different windows: each
server has logging to the console to show what is happening.

Run the patch log server which keeps persisetnt logs on-disk:

```
 java -jar rdf-delta-server.jar --base DeltaSever
```

Run one Fuseki server:

```
 java -jar delta-fuseki.jar --conf config-1.ttl.
```

`config-1.ttl` is a Fuseki configuration file with an in-memory dataset
that logs changes to an patch server running at `http://localhost:1066/`.

The SPARQL endpoint is `http://localhost:3030/ds1`.

The patch log short name is `ABC`. This can be seen in
`DeltaServer/ABC/` which is the patch servers work area for the log.

## Actions

First, see that the triple store is empty:

```
  s-query --service http://localhost:3030/ds1 --output=text 'SELECT * { ?s ?p ?o }'
```

If you don't have the 
[SOH tools](https://jena.apache.org/documentation/serving_data/) then
you can use `curl`:

```
  curl --data 'query=SELECT * { ?s ?p ?o }' --data 'output=text' http://localhost:3030/ds1
```

`output=text` requests that the Fuseki return results in a text-readable
format.

```
    -------------
    | s | p | o |
    =============
    -------------
```
No triples.

Now send a patch to the patch server directly:

```
  dcmd add --server http://localhost:1066/ --log ABC patch.rdfp
```
`patch.rdfp` is a small example patch file. 

Query the Fuseki server again:
```
    --------------------------------------------------
    | s                  | p                  | o    |
    ==================================================
    | <http://example/s> | <http://example/p> | 1816 |
    | <http://example/s> | <http://example/q> | _:b0 |
    --------------------------------------------------
```

To show master-master replicated Fuseki server, start a second one on a
different port:

```
  java -jar delta-fuseki.jar -port 3535 --conf config-2.ttl
```

To help further distriniguish the server for this demo, the dataset is
called `ds2`. It woudl normally be the same name if the two servers are
behidn a load balancer.

The second  SPARQL endpoint is `http://localhost:3535/ds2`.

Query that server and it should already the same data:

```
  s-query --service http://localhost:3535/ds2 --output=text 'SELECT * { ?s ?p ?o }'
```
```
    --------------------------------------------------
    | s                  | p                  | o    |
    ==================================================
    | <http://example/s> | <http://example/p> | 1816 |
    | <http://example/s> | <http://example/q> | _:b0 |
    --------------------------------------------------
```

Now update one server and query the other

```
  s-update --service http://localhost:3535/ds2 \  
    'DELETE { ?s ?p 1816 } INSERT { ?s ?p 1850 } WHERE { ?s ?p 1816 }'
```

```
  s-query --service http://localhost:3030/ds1 --output=text 'SELECT * { ?s ?p ?o }'
```
to get 
```
    --------------------------------------------------
    | s                  | p                  | o    |
    ==================================================
    | <http://example/s> | <http://example/p> | 1850 |
    | <http://example/s> | <http://example/q> | _:b0 |
    --------------------------------------------------
```

# <a name="fuseki-config"></a>The Fuseki Configuration file


This is the file Fuseki configuration file use above, with additonal
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
    fuseki:name                        "ds1" ;
    fuseki:serviceQuery                "sparql" ;
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
    delta:changes  "http://localhost:1068/" ;
    
    ## The log name for this dataset.
    delta:patchlog "ABC";

    ## The client library workarea.
    ## It can not be shared between servers.

    delta:zone "target/Zone1";

    ## The type of storage.
    ## Persistent options include "tdb" and "tdb2".
    delta:storage "mem";
    .
```
