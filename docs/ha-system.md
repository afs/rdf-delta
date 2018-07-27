---
layout: doc
title: High Availability RDF Delta System
nav_text: HA Tutorial
###section: 6
---
This tutorial covers setting up RDF Delta to provide high availability
for  [Apache Jena
Fuseki](https://jena.apache.org/documentation/fuseki2/index.html) and
ensuring the RDF Delta system is itself highly available.

The tutorial starts with setting up a simple, not fault tolerant, setup
and proceeds in stages to upgrade the components to make them reliable
in the presence of failures.

Failures aren't just crashes and network problems; they also include
planned admin operation such as upgrading a server or updating an
operating system.

Replicating the Fuseki triple store also provides for horizontal
scalability, being able to respond to more requests per unit time, or to
provide different servers as provisioning for different applications.

A fully resilient system involves several separate machines and is
complicated to operate and maintain. Application needs may be better
met with simpler setups at reduced reliability. For example, using a
file-back patch log server, together with file system backup, can
provide replication for Fuseki for load sharing and high available of
the Fuseki servers at the cost of not being able to update data when the
patch log server is no available.  Because restart of patch log server is
quick, this may be an acceptable trade-off.

For demonstration/experimentation purposes, this tutorial runs all the
servers on the local machine.  In a production environment, the different
servers will need to be run on separate machines, each on separate
hardware.

* [Obtaining the Software](#software)
* [The Delta HA Stack](#ha-stack)
* [A simple setup](#simple-stack)
* [Run Two Fuseki Servers](#two-fuseki)
* [File-based persistent patch storage](#file-backed-patch-store)
* [Adding High Availability Components](#ha-stack)
* [Failover Fuseki Configuration](#fuseki-failover)
* [Using S3 for patch storage](#patch-storage-s3)

## Obtaining the Software {#software}

The software is available as a single package from:

[`central.maven.org::rdf-delta-dist`](http://central.maven.org/maven2/org/seaborne/rdf-delta/rdf-delta-dist/)

Download the latest version and unpack the zip file: there will be a
directory `rdf-delta-VER`.

This includes the scripts and configuration files used in this tutorial
in the `Tutorial` directory.

The tool to run command and servers on the command line is the script
`dcmd`. Commands have the form <tt>dcmd <i>SubCmd</i></tt> and support `--help` (`-h`) for a
brief summary. The page ["Delta - Command Line Tools"](cmds) has more details.

The RDF Delta Patch Log Server is in the self-contained jar
`delta-server.jar`. <br/>
The Fuseki server packaged with the RDF Delta client libraries is `delta-fuseki.jar`

In maven coordinates: `org.seaborne.rdf-delta:rdf-delta-server:VER`
and `org.seaborne.rdf-delta:rdf-delta-fuseki-server:VER`.

## The Delta HA Stack {#ha-stack}

The end-to-end system has components for

1. the Apache Jena Fuseki Servers
2. the RDF Delta Patch Servers
3. the index of patch logs
4. the storage layer of the patched themselves

The Fuseki servers each have their own copies of a database for RDF
Dataset they provide. All SPARQL operations happen locally in a
transaction.  When changes are made, they are recorded locally as [RDF
Patches](rdf-patch). The database has its own [RDF Patch
Log](rdf-patch-logs). At the end of the operation (the HTTP request),
any changes are sent to a [patch log server](delta-patch-server) and made
safe. Only when this has happened does the triple store declare the
transaction as committed.  It is the state of the RDF Patch Log that
determines the state of the cluster.

When operations arrive at Fuseki server, the server first checks its
local database is up-to-date and, if it isn't, fetches any patches
to make sure it is at the latest version.

A "patch log" is a sequence of patches in the order that changes happened. The
patch log does not branch. In order to add a new patch to the log, the Fuseki server
has to say what it thinks is the head of the log. It is doesn't match,
the update to the log is rejected and the transaction in the Fuseki
server aborts with no changes made.

The [RDF Patch Log](rdf-patch-logs) consists of the index of patch logs,
that gives the log its structure, and the body of the patches.  Some
setups combine these two parts, while other setups store the bulk
patches separately from the index.  This is transparent to [patch log
server clients (the Fuseki servers).  The combination of patch log
index and patch storage is called a "patch store".

## Simple setup {#simple-stack}

In the first preliminary setup, we use a in-memory log index and patch
storage, in a single patch server, then make do some operations on the
patch log server from the command line.  In the next section, we will run
two Fuseki servers with in-memory databases. This setup shows changes to the RDF
datasets being replicated across two Fuseki servers.

### Run a patch log server

To run the patch log server with a non-persistent in-memory patch store,
start a separate terminal window, go to the installation directory and run:
```
# In a separate terminal window:
dcmd server -mem
```

The server logs to stdout. The command does not return until the server
stops. To stop the server, kill it with a control-C.

The patch log server is a self-contained jar and can be run directly:
```
java -jar delta-server.jar --mem
```

Example output:
```
[2018-07-27 12:31:54] INFO  Delta                : Delta Server port=1066
[2018-07-27 12:31:55] INFO  Delta                :   No data sources
[2018-07-27 12:31:55] INFO  Delta                : DeltaServer starting
```
The server is running and accepting request on port 1066. There are no
logs.

### Run delta commands

There are a number of command line tools for working with server. One is to list the logs:
```
dcmd ls --server http://localhost:1066/
```
```
-- No logs--
```
The server will also log operations.

We can make a new patch log. A log has a name (required) and URI (optional,
one is generated if not supplied).  The name "ABC" is used again later
in the tutorial.
```
dcmd mk --server http://localhost:1066/ ABC
```
```
Created [id:504b9e, ABC, <http://delta/ABC>]
```
and now `dcmd ls` shows one empty log:
```
[id:504b9e ABC <http://base/ABC> [empty]]
```
Every log and every patch has a unique id. `id:504b9e` is a shortened
form.

There is an example patch in the tutorial directory so let's add it to the log:
```
dcmd append --server http://localhost:1066/ --log ABC patch.rdfp
```
```
Version = 1
```
and `dcmd ls` shows:
```
[id:504b9e ABC <http://base/ABC> [ver:1,ver:1] id:3e9809]
```
There is one patch, `id:3e9809`. The first `ver` is the minimum version number,
the second `ver` the head of the log. Because there is only one patch at the moment,
the log earliest version and log head version are the same.

## Run Fuseki Servers {#two-fuseki}

If you run fuseki immediately after the previous section, the Fuseki
server will sync to the log because the Fuseki configuration file names
the log to use as "ABC", the same as above:

Start a fuseki server with a simple in a separate terminal window:
```
# In a separate terminal window:
dcmd fuseki --port 3031 --conf fuseki-config.ttl
```
```
[2018-07-27 12:38:42] INFO  Delta                : Delta Patch Log Servers: [http://localhost:1066/]
[2018-07-27 12:38:42] INFO  Delta                : Sync: Versions [<init>, ver:1]
[2018-07-27 12:38:42] INFO  HTTP                 : Fetch request: id:504b9e version=1 [http://localhost:1066/504b9ec3-6d27-4358-9b2a-b49230df0fd8/1]
[2018-07-27 12:38:42] INFO  Server               : Apache Jena Fuseki (basic server) 3.8.0
[2018-07-27 12:38:42] INFO  Server               : Configuration file fuseki-config.ttl
[2018-07-27 12:38:42] INFO  Server               : Path = /ds1; Services = [data, quads, upload, query, sparql, update, get]
[2018-07-27 12:38:42] INFO  Server               :   Memory: 7.8 GiB
[2018-07-27 12:38:42] INFO  Server               :   Java:   10.0.1
[2018-07-27 12:38:42] INFO  Server               :   OS:     Linux 4.15.0-29-generic amd64
[2018-07-27 12:38:42] INFO  Server               :   PID:    8608
[2018-07-27 12:38:42] INFO  Server               : Start Fuseki (port=3031)
```

The configuration sets the patch log for the dataset to be `ABC`, which already exists,
and the Fuseki server has synchronized.

If you query the server ("s-query" is a command line program to send
SPARQL queries, See
[SOH](http://jena.apache.org/documentation/fuseki2/soh.html for
details)

```
s-query --service=http://localhost:3031/ds --output=text 'SELECT * { ?s ?p ?o }'
```
```
--------------------------------------------------
| s                  | p                  | o    |
==================================================
| <http://example/s> | <http://example/p> | 1816 |
| <http://example/s> | <http://example/q> | _:b0 |
--------------------------------------------------
```

Any way to send SPARQL queries over HTTP will work, for example:
```
curl -d query='SELECT * {?s ?p ?o}' -d 'output=text' http://localhost:3031/ds
```

Now we run a second Fuseki server on a different port:
```
dcmd fuseki --port 4042 --conf fuseki-config.ttl
```

We can update one server and query the other to see the changes:

<pre>
s-update --service=http://localhost:<b>4042</b>/ds 'INSERT DATA { <urn:x:s> <urn:x:p> <urn:x:o> }'
s-query --service=http://localhost:<b>3031</b>/ds --output=text 'SELECT * { ?s ?p ?o }'
-------------------------------------------------------
| s                  | p                  | o         |
=======================================================
| <http://example/s> | <http://example/p> | 1816      |
| <http://example/s> | <http://example/q> | _:b0      |
| <urn:x:s>          | <urn:x:p>          | <urn:x:o> |
-------------------------------------------------------
</pre>

The log now has two patches in it:
```
dcmd ls --server http://localhost:1066/
```
```
[id:504b9e ABC <http://base/ABC> [ver:1,ver:2] id:b6cadc]
```

## File-based persistent patch storage {#file-backed-patch-store}

```
dcmd server --base DIRECTORY
```

## Adding High Availability Components {#ha-stack}

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

1. Zookeeper is used to store the patches themselves and Zookeeper only
handles small files.  A full production system would use a patch storage
layer such as AWS S3.

2. The Zookeeper servers are run in the same JVM as the RDF Delta Patch
Servers as a self-contained Zookeeper ensemble. An external Zookeeper
ensemble can be used if desired.

## Failover Fuseki Configuration {#fuseki-failover}

The last step is to configure each Fuseki server to failover if it can't
contact one of th RDF Delta Patch Log Servers.

This is done by changing the `delta:changes` property to be a list of
patch servers.  Whenever the Fuseki server fails to contact one of the
patch servers, it wil switch to using another patch server.

```
...
<#dataset> rdf:type delta:DeltaDataset ;
    ## List of Delta Patch Servers
    delta:changes  ("http://localhost:1071/" "http://localhost:1072/"  "http://localhost:1073/") ;
...
```

## Using S3 for Patch Storage {#patch-storage-s3}

Patches of any size can be stored in S3, or any system that provides the
AWS S3 API (for example, there is an adapter for Apache Cassandra).

```
dcmd server -zk=... --s3Bucket=BUCKET  --s3Keys=KEYS --s3Region=REGION
```
where `BUCKET` is the S3 bucket name, `KEYS` is a AWS credential
proerties file, if not supplied the satndard defaul S3 autehntication
mechanism is used, and the `REGION` is an AWS region name such as
"us-east-1".
