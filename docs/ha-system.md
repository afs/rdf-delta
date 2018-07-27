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
  * [Run a patch log server](#patch-log-server)
  * [Run Delta commands](#delta-cmds)
  * [Connect a Fuseki server](#single-fuseki)
* [Failover Fuseki Configuration](#fuseki-failover)
* [File based persistent patch storage](#file-patch-store)
* [Adding High Availability Components](#ha-patch-store)
* [Using S3 for patch storage](#patch-storage-s3)

## Obtaining the Software {#software}

The software is available as a single package from:

[`central.maven.org::rdf-delta-dist`](http://central.maven.org/maven2/org/seaborne/rdf-delta/rdf-delta-dist/)

Download the latest version and unpack the zip file: there will be a
directory `rdf-delta-VER`.

Set the environment variable `DELTA_HOME` to this directory. It is used to find
binaries to run for the command show later.

This includes the scripts and configuration files used in this tutorial
in the `Tutorial` directory.

The tool to run command and servers on the command line is the script
`dcmd`. Commands have the form <tt>dcmd <i>SubCmd</i></tt> and support `--help` (`-h`)
for a brief summary. The page ["Delta - Command Line Tools"](cmds) has more details.

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

### Run a patch log server {#patch-log-server}

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

### Run delta commands {#delta-cmds}

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

### Connect a Fuseki server {#single-fuseki}

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

## Failover Fuseki Configuration {#fuseki-failover}

Now we run a second Fuseki server on a different port:
```
dcmd fuseki --port 4042 --conf fuseki-config.ttl
```

We can update one server and query the other to see the changes:

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

The log now has two patches in it:
```
dcmd ls --server http://localhost:1066/
```
```
[id:504b9e ABC <http://base/ABC> [ver:1,ver:2] id:b6cadc]
```

## File-based persistent patch storage {#file-patch-store}

The patch log can be made persistent so that the patch log server can be stopped
and started using a file-back patch store.

Stop any other patch servers and Fuseki servers running from earlier on.

The directory for the patch store must be create first.

```
mkdir PatchStore
dcmd server --base PatchStore
```
and the simple patch added:
```
dcmd append --server http://localhost:1066/ --log ABC patch.rdfp
```
```
Version = 1
```
The patch will be stored in file `PatchStore/ABC/patch-0001`

There is only a single copy of the patch store and any file storage may be lost
so it is better to highly reliable file system where possible.

The patch store can be backed up by backing up the `PatchStore` directory.

## Adding High Availability Components {#ha-patch-store}

So far, there has been only one patch log server. While the file-back
patch store keeps the patches safe, it is not a high-availability solution.

RDF Delta provides a high-availability, replicated patch log server
system using Apache Zookeeper.  There needs to be 3 instances of Zookeeper
to survive the loss of one server while continuing to
provide operation if the network becomes unreliable. 3 servers means
that in a working partition of the system there is a majority quorum of 2.
If there were only two servers, a "split brain" situation is possible
where the two servers can not contact each other but both are running
and believe they are the master quorum leader.

In this tutorial, Zookeeper is used to store the patches themselves and Zookeeper only
handles small files.  A full production system would use a patch storage
layer such as AWS S3.

The Zookeeper servers are run in the same JVM as the RDF Delta Patch
Servers as a self-contained Zookeeper ensemble. An external Zookeeper
ensemble can be used if desired.

The running Zookeeper servers store their own state to disk.
First, copy the tutorial configuration into a working area because it will get modified:
```
cp -r zk-example zk
```
There are 3 sub-directories, `zk/zk1`, `zk/zk2`, `zk/zk3`, one for each server.

In separate separate terminal windows: there is a instance-specific configuration "zoo.cfg",
a different one in each directory.  Each patch log server runs on a different port,
 1071, 1072 and 1073.

```
cd zk1
dcmd server --port 1071 --zk=localhost:2181,localhost:2182,localhost:2183 --zkCfg=./zoo.cfg
```
```
cd zk2
dcmd server --port 1072 --zk=localhost:2181,localhost:2182,localhost:2183 --zkCfg=./zoo.cfg
```
```
cd zk3
dcmd server --port 1073 --zk=localhost:2181,localhost:2182,localhost:2183 --zkCfg=./zoo.cfg
```

NB Zookeeper logging is quite detailed.  The tutorial setup has a `logging.properties`
file for each server instance and logging is turned down to a minimal level.
It may be necessary to increase the logging to diagnose problems.


Alternatively, run the script "`../dzk-server`" each of zookeeper server directories.
It will determine the service instance number.

To reset the system, delete the `zk` directory and copy 'zk-example` again.
To reset just the zookeeper state, delete the three directories
`zk{1,2,3}/ZkData/version-2`.

```
dcmd ls --server http://localhost:1071
```
```
-- No logs --
```
Create a log on one server (port 1073):
```
dcmd mk --server http://localhost:1073 ABC
```
```
Created [id:de5992, ABC, <http://delta/ABC>]
```
and see it is present on another (port=1071):
```
dcmd ls --server http://localhost:1071
```
```
[id:de5992 ABC <http://delta/ABC> [empty]]
```

Now run two Fuseki server on different ports.
These server have a persistent database, which is described below.

```
cd ../fuseki1
dcmd fuseki --port 3033 --conf config.ttl
[2018-07-27 15:40:49.692] INFO  Delta                : Delta Patch Log Servers: [http://localhost:1071/, http://localhost:1072/, http://localhost:1073/]
[2018-07-27 15:40:50.403] WARN  Delta                : Sync: Asked for no patches to sync
[2018-07-27 15:40:50.474] INFO  Server               : Apache Jena Fuseki (basic server) 3.8.0
[2018-07-27 15:40:50.476] INFO  Server               : Configuration file config.ttl
[2018-07-27 15:40:50.477] INFO  Server               : Path = /ds; Services = [data, quads, upload, query, sparql, update, get]
[2018-07-27 15:40:50.479] INFO  Server               :   Memory: 7.8 GiB
[2018-07-27 15:40:50.480] INFO  Server               :   Java:   10.0.1
[2018-07-27 15:40:50.480] INFO  Server               :   OS:     Linux 4.15.0-29-generic amd64
[2018-07-27 15:40:50.480] INFO  Server               :   PID:    19872
[2018-07-27 15:40:50.500] INFO  Server               : Start Fuseki (port=3033)
```
```
cd ../fuseki2
dcmd fuseki --port 3055 --conf config.ttl
[2018-07-27 15:43:12.102] INFO  Delta                : Delta Patch Log Servers: [http://localhost:1071/, http://localhost:1072/, http://localhost:1073/]
[2018-07-27 15:43:12.112] INFO  Zone                 : Connection : /home/afs/ASF/rdf-delta/rdf-delta-examples/Tutorial/zk/fuseki2/Zone/ABC
[2018-07-27 15:43:12.122] WARN  DataState            : No version: {  "version" : 0 , "id" : "" , "name" : "ABC" , "datasource" : "de599246-7d17-404a-a57c-23d92deffcb7" , "storage" : "TDB" , "uri" : "http://delta/ABC" }
[2018-07-27 15:43:12.362] WARN  Delta                : Sync: Asked for no patches to sync
[2018-07-27 15:43:12.437] INFO  Server               : Apache Jena Fuseki (basic server) 3.8.0
[2018-07-27 15:43:12.439] INFO  Server               : Configuration file config.ttl
[2018-07-27 15:43:12.440] INFO  Server               : Path = /ds; Services = [data, quads, upload, query, sparql, update, get]
[2018-07-27 15:43:12.443] INFO  Server               :   Memory: 7.8 GiB
[2018-07-27 15:43:12.444] INFO  Server               :   Java:   10.0.1
[2018-07-27 15:43:12.444] INFO  Server               :   OS:     Linux 4.15.0-29-generic amd64
[2018-07-27 15:43:12.444] INFO  Server               :   PID:    20098
[2018-07-27 15:43:12.471] INFO  Server               : Start Fuseki (port=3055)
```

You can now update one server and see the changes in the other as before or you can send
a patch to the patch log server ensemble and it will appear in all servers when they
next sync up.

```
dcmd append --server http://localhost:1072/ --log ABC patch.rdfp
```
or (SPARQL Graph Store Protocol) for some RDF data:
```
s-put http://localhost:3033/ds default data.ttl
```
or plain HTTP:
```
 curl -T data.ttl --header 'Content-type:text/turtle'  http://localhost:3033/ds
```

The Fuseki configuration in this section above has local persistent database.
When the Fuseki server starts up it uses this database, together with a note of
the version of the database and start from there, rather than rebuilding the database
from scratch each time it starts up.

```
...
<#dataset> rdf:type delta:DeltaDataset ;
    ## List of Delta Patch Servers
    delta:changes  ("http://localhost:1071/" "http://localhost:1072/"  "http://localhost:1073/") ;
...
```

Full details of the RDF Delta configuration file for Apache Jena Fuseki
are given [here](delta-fuseki-config).

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
