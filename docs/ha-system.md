---
layout: doc
title: High Availablity RDF Delta System
nav_text: HA RDF Delta
###section: 6
---
This section describes a system where every component is replicated. It
consists of 2 [Apache Jena Fuseki](https://jena.apache.org/documentation/fuseki2/index.html)
triple stores, with 3 RDF Delta Servers with [Apache
Zookeeper](https://zookeeper.apache.org/) providing the system-wide
replicated patch state.

See the [High Availablity Apache Jena Fuseki](ha-fuseki.html) for a
simpler, file based example for high availability Fuseki.

This example start by setting up the different components of the system
in a simple fashion, the replacing each layer of the stack with a full
replicated component.

The example has two simpliifications:

1. Zookeeper is used to store the patches themselves and Zookeeper only
handles small files.  A full production system would use a patch storage
layer such as AWS S3.

2. The Zookeeper servers are run in the same JVM as the RDF Delta Patch
Servers as a self-contained Zookeeper ensemble. An external Zookeeper
ensemble can be used if desired.

* [Obtain the Software](#software)
* [The Delta HA Stack](#ha-stack)
* [Simple setup](#simple-stack)
* [Adding High Availability Components](#ha-stack)
* [Fuseki Configuration for Switchover](#fuseki-config)

# Obtain the Software {#software}

The software is available as a single package from:

[central.maven.org::rdf-delta-dist](http://central.maven.org/maven2/org/seaborne/rdf-delta/rdf-delta-dist/)

Download the latest version and unpack the zip file: there will be a
directory `rdf-delta-VER`.

This includes the scripts and configuration files used in this example.

For demonstration/experimentation purposes, this "getting started" guide
runs all the servers on the local machine.  In a production enviroment,
the different servers should be run on separate machines, each on
separate hardware.

# The Delta HA Stack {#ha-satck}

The system has components for 
1. the Apache jena Fuseki Servers
2. the RDF Delta Patch Servers
3. the Apache Zookeeper for the replicated index of patch logs
4. a storage layer for the bulk patches.

In the first preliminary setup, we use a in-memory log index and patch
storage, in a single patch server.

Then we relace that with a patch server running a single zookeeper
instance.

Finally, we run 3  patch servers, each with a co-hosted zookeeper server
and the zookeeper servers form a high-availablity database.

We use 3 instances to survive the loss of any one server and also to
continue operation if the network becomes unreliable. 3 servers means
that a workign system has a majority quorum of 2. With onyl two servers,
a "split brain" situation is possible where they can not contact each
other but both are running and belive they are the master quorum
leader. With 3, network parition means that 2 servers must be in one
side or the other.  No majority quorum is possible at all, the system
stops accepting updates.

# Simple setup {#simple-stack}

# Adding High Availability Components {#ha-stack}

# Fuseki Configuration for Switchover {#fuseki-config}
