---
layout: doc
title: High Availablity RDF Delta System
nav_text: HA Tutorial
###section: 6
---
This tutorial covers setting up RDF Delta to provide high availability
for  [Apache Jena
Fuseki](https://jena.apache.org/documentation/fuseki2/index.html) and
ensuring the RDF Delta system is itself highly available.

The tutorial start with setting up a simple, not fault tolerant, setup
and proceeds in stages to upgrade the components to make them reliable
in the presence of failures.

Such failures aren't just crashes and network problems; it also include
planned admin operation such as upgrading a server or updating an
operating system.

A fully resilient system involves several separate machines and is
scomplciated to operate and maintain. Application needs may be better
met with simpler setups at reduced reliability. For example, using a
file-back patch log server, together with file system backup, can
provide replication for Fuseki for load sharing and high available of
the Fuseki servers at the cost of not being able to update data when the
patch log server is no available.  Because restart of patch log server is
quick, this may be an acceptable tradeoff.

For demonstration/experimentation purposes, this "getting started" guide
runs all the servers on the local machine.  In a production enviroment,
the different servers wil need to be run on separate machines, each on
separate hardware.

1. Zookeeper is used to store the patches themselves and Zookeeper only
handles small files.  A full production system would use a patch storage
layer such as AWS S3.

2. The Zookeeper servers are run in the same JVM as the RDF Delta Patch
Servers as a self-contained Zookeeper ensemble. An external Zookeeper
ensemble can be used if desired.

* [Obtaining the Software](#software)
* [The Delta HA Stack](#ha-stack)
* [A simple setup](#simple-stack)
* [File-based persistent patch storage](#file-backed-patch-store)
* [Adding High Availability Components](#ha-stack)
* [Failover Fuseki Configuration](#fuseki-failover)

# Obtaining the Software {#software}

The software is available as a single package from:

[central.maven.org::rdf-delta-dist](http://central.maven.org/maven2/org/seaborne/rdf-delta/rdf-delta-dist/)

Download the latest version and unpack the zip file: there will be a
directory `rdf-delta-VER`.

This includes the scripts and configuration files used in this example.

The tool to run command and servers on the command line is the script
`dcmd`. All commands are `dcmd <i>SubCmd<i>` and support "-h" for a
brief summary. The page [Delta - Command Line Tools](cmds) has more details.

The RDF Delta Patch Log Server is in the self-contained jar
`delta-server.jar`. 

The Fuseki server packaged with the RDF Delta client libraries is `delta-fuseki.jar`

In maven coordinates: `org.seaborne.rdf-delta:rdf-delta-server:VER`
and `org.seaborne.rdf-delta:rdf-delta-fuseki-server:VER`.

@@ Revise when the examples layout is finalized.

`config-1.ttl`, `config-2.ttl` -- the Fuseki confiuration files for the
simple setup.

`zk-example` -- the zookeeper based example

# The Delta HA Stack {#ha-satck}

The end-to-end system has components for 

1. the Apache Jena Fuseki Servers
2. the RDF Delta Patch Servers
3. the index of patch logs
4. the storage layer of the patched themselves

The Fuseki servers have their own copies of a database for each RDF
Dataset they provide. All SPARQL operations happen locally in a
transaction.  When changes are made, they are recorded locally as [RDF
Patches](rdf-patch). Each database has its own [RDF Patch
Log](rdf-patch-logs). At the end of the operation (the HTTP request),
they are sent to a [patch log server](delta-patch-server) and made
safe. Only when this has happened does the triple store declare the
transaction as committed.  It is the state of the RDF Patch Log that
determines the state of the cluster.

When operations arrive at Fuseki server, the server first checks its
locl database is up to date and if it isn't, makes sure it is brought
upto date before letting the operation continue.

A "log" is a sequence of patches in order thaty happened. The log does
not branch; in order to add a new patch to the log, the Fuseki server
has to say what it thinks is the head of the log. It is doesn't match,
the update to the log is rejected and the transaction in the Fuseki
server aborts with no changes made.

The [RDF Patch Log](rdf-patch-logs) consist of the index, that gives the
log its structure, and the body of the patches, which can be large. Some
setups combine these two parts, while other setups store the bulk patches
separately from the index.  This is transparent to [patch log server
clioents (the Fuseki servers).  The combination of patch log index and patch
storage is called a "patch store".

-- pictures --

# Simple setup {#simple-stack}

In the first preliminary setup, we use a in-memory log index and patch
storage, in a single patch server.  This is not fault-tolerant in any
way but it is simple to run for experimentation. We also run the Fuseki
servers with in-memory databases. This setup shows changes to the RDF
datasets being replicated across two Fuseki servers.

## Run a patch log server

To run the patch log server with a non-persistent in-memory patch store,
start a separate terminl window, go to the istallation directory and run:
```
dcmd server -mem
```

The server logs to stdout. The command does not return until the server
stops. To stop the server, kill it with a control-C.


The patch log server is a self-contained jar and can be run directly:
```
 java -jar delta-server.jar --mem
```

Example logging output:
```
[2018-07-26 13:12:08,129] INFO  Delta                :: Delta Server port=1066
[2018-07-26 13:12:08,188] INFO  Delta                ::   No data sources
[2018-07-26 13:12:08,215] INFO  Delta                :: DeltaServer starting
```
The server is running and accepting request on port 1066. There are no
logs.

## Run delta commands

There are a number of command line tools for working with server:
```
dcmd ls --server http://localhost:1066/
```
```
-- No logs--
```
The server will also log operations.

Make a new patch log: logs have a name (required) and URI (optional,
indented if not supplied.).
```
dcmd mk --server http://localhost:1066/ ABC
```
```
Created [id:01711e, ABC, <http://base/ABC>]
```
and now `ls` shows:
```
[id:01711e ABC <http://base/ABC> [<init>,<init>] <no patches>]
```
Every log and every patch has a unique id. `id:01711e` is a shortened
form.  ` [<init>,<init>] <no patches>` shows there are no patches in the
log.

There is a trivial patahc in the distriution solets add it to the log:

```
dcmd append --server http://localhost:1066/ --log ABC patch.rdfp
```
and now `dcmd ls` shows:
```
[id:01711e ABC <http://example/ABC> [ver:1,ver:1] id:384c0b]
```
onepatch at version 1. The first `ver` is the minimum version number,
the second `ver` the head of the log, which chnages as patches are
added, and `id:384c0b` is the current patch at the head of the log.

## Run a Fuseki Server

If you run fuseki immediately after the previous section, the Fuseki
server will sync to the log because the Fuseki configuration file names
the log to use as "ABC", the same as above:

Start a fuseki server with a simple in a separate terminal window:
```
dcmd fuseki --port 3031 --conf config-1.ttl
```
@@ fixme for 0.5.1
```
[2018-07-26 13:39:14,165] INFO  Delta                :: Delta Patch Log Servers: [http://localhost:1066/]
[2018-07-26 13:39:14,452] INFO  Delta                :: Sync: Versions [%d, %d] [Ljava.lang.Object;@7479b626
[2018-07-26 13:39:14,452] INFO  HTTP                 :: Fetch request: id:da0969 version=1 [http://localhost:1066/da0969d5-a70f-41a2-8101-6dc2a5217eb3/1]
[2018-07-26 13:39:14,545] INFO  Server               :: Apache Jena Fuseki (basic server) 3.8.0
[2018-07-26 13:39:14,547] INFO  Server               :: Configuration file config-1.ttl
[2018-07-26 13:39:14,547] INFO  Server               :: Path = /ds; Services = [data, quads, upload, query, sparql, update, get]
[2018-07-26 13:39:14,558] INFO  Server               ::   Memory: 7.8 GiB
[2018-07-26 13:39:14,558] INFO  Server               ::   Java:   10.0.1
[2018-07-26 13:39:14,558] INFO  Server               ::   OS:     Linux 4.15.0-29-generic amd64
[2018-07-26 13:39:14,558] INFO  Server               ::   PID:    32259
[2018-07-26 13:39:14,580] INFO  Server               :: Start Fuseki (port=3031)
```

and if you query the server ("s-query" is a command line program to send
SPARQL queries, See
[SOH](http://jena.apache.org/documentation/fuseki2/soh.html) for
details; any way to send SPARQL queries over HTTP will work.
`--output=text`, or HTTP `&format=text` askes for text outout.

```
s-query --service=http://localhost:3031/ds --output=text 'SELECT * { ?s ?p ?o }'
--------------------------------------------------
| s                  | p                  | o    |
==================================================
| <http://example/s> | <http://example/p> | 1816 |
| <http://example/s> | <http://example/q> | _:b0 |
--------------------------------------------------
```

## Run a second Fuseki Server

Run a 
```
dcmd fuseki --port 4042 --conf config-2.ttl
```

Note: the servers different port number.
  
Now update one server and query the other:


```
s-update --service=http://localhost:4042/ds 'INSERT DATA { <urn:x:s> <urn:x:p> <urn:x:o> }'
s-query --service=http://localhost:3031/ds --output=text 'SELECT * { ?s ?p ?o }'
-------------------------------------------------------
| s                  | p                  | o         |
=======================================================
| <http://example/s> | <http://example/p> | 1816      |
| <http://example/s> | <http://example/q> | _:b0      |
| <urn:x:s>          | <urn:x:p>          | <urn:x:o> |
-------------------------------------------------------
```


or if using a clean-start with no data from the "command line tools" section:
```
-------------------------------------
| s         | p         | o         |
=====================================
| <urn:x:s> | <urn:x:p> | <urn:x:o> |
-------------------------------------
```
and you can send the patch with `dcmd append` as before and see it
appear on both servers.

# File-based persistent patch storage {#file-backed-patch-store}

```
dcmd server --base DIRECTORY
```

# Adding High Availability Components {#ha-stack}

So far, there has been only one patch log server. Whiel the file-back
patch store keeps the patches safe, it is not high-availility. The file
storage may be lost (it is better to use a higly reliabel file system
than a local server file sytem) and the server may need restarting.
continuous operation.

RDF Delta provides a high-availability, replicated patch log serer
system using Apache Zookeeper for a high-availablity database.

We use 3 instances to survive the loss of any one server and also to
continue operation if the network becomes unreliable. 3 servers means
that a working system has a majority quorum of 2. With onyl two servers,
a "split brain" situation is possible where they can not contact each
other but both are running and belive they are the master quorum
leader. With 3, network parition means that 2 servers must be in one
side or the other.  No majority quorum is possible at all, the system
stops accepting updates.

@@ limitations of zookeeper
@@ S3.


# Failover Fuseki Configuration {#fuseki-failover}

```
...
<#dataset> rdf:type delta:DeltaDataset ;
    ## List of Delta Patch Servers
    delta:changes  ("http://localhost:1071/" "http://localhost:1072/"  "http://localhost:1073/") ;
...
```
