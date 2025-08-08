---
layout: doc
title: RDF Delta Patch Server
#nav_text: RDF Delta Patch Server
#section: 5
---

The RDF Delta Patch Server provides the backend for a fault-tolerant cluster of
machines such as Fuseki servers.

The patch server provides a number of different configuration options.

* Local file system using [RocksDB](https://rocksdb.org/) for patch storage.
* Fully fault tolerant cloud form, with multiple RDF Delta servers and an object store for patch storage
* Fully fault tolerant cloud form, with multiple RDF Delta servers with separate Zookeeper servers.
* Local file system using plain files for patch storage
* Single server, in-memory patch logs.

When the server starts with a local mode, the storage choice (plain file, or
local database) for existing patch logs is preserved. The server setup influences
the storage for patch logs when they are created.

The fault tolerant form uses [Apache Zookeeper](http://zookeeper.apache.org/) as
its fault-tolerant storage for indexes and Zookeeper for storing the content of patches.

The recommended deployment choices are using RocksDB storage (`--store`), with
either a filesystem which should be on reliable storage or a backed up
filesystem, for smaller installations, and fully fault tolerant installation for
24x7 availability.

The in-memory option is useful for testing and development because it
starts quickly and can run in the testing JVM process.

The plain file configuration is useful to explore how the system works.

## Choosing the Configuration.

The fully fault-tolerant configuration provides the best high availability
system but requires more machines and is more complex to administer.

A single-server delta patch does not run arbitrary application code, which makes
it more reliable, and it starts quickly.  If a patch can not be stored, the
Fuseki server will not commit its transaction but the Fuseki servers continue
proving query operations. Restarting a patch server does not require other
servers ot be restarted.

Where 24x7 continuous operation is not required, the simpler configuration of a
single patch server may be more appropriate.  Coupled with a file system that is
itself sufficiently reliable (e.g. a disk array or RAID system), or run on a
server machine whose filesystem is regularly backed up, it does provide a high degree of
availability.

## Server arguments

| Argument  | Configuration |
|-----------|---------|
| `--store` | Local database storage  |
| `--base`  | Plain file storage      |
| `--mem`   | In-emory, development/testing mode |
| `--zk`    | Zookeeper connection string<br/>"host1:port1,host2:port2,host3:port3"|

### High Available Additional Configuration

The Embedded Zookeeper form has several additional arguments:

| Argument for <br/>Zookeeper   |  |
|-----------|---------|
| `--zkConf` | Configuration file |
| `--zkData`  | Storage for the embedded Zookeeper |
| `--zkPort` | Port for the embedded Zookeeper | |

When using an external Zookeeper connection, the following arguments are available:

| Argument for <br/>External<br/>Zookeeper   |  |
|-----------|---------|
| `--zkRootDir` | Root directory to use in<br/>Zookeeper to store patch<br/>info _(defaults to `/delta`<br/>and is ignored if embedded<br/>Zookeeper is used)_

## Examples

### Run a single server

This example is for running a single patch log server on the default port (1066)
with a RockDB database for patches. Patch logs are stored in the directory
"Delta" which must exist.

<pre>
    dcmd server --store Delta
</pre>

### Run a high availability patch log server 

This example is for a high availability patch log service, consisting of 3 patch
log servers, each using using the embedded Apache Zookeeper.

The three patch log server should be run on different machines.

The connection string for Zookeeper lists the machiens and the Zookeeper port
used for coordination. Suppose we have machines "server1", "server2", "server3"
with zookeeper running on port 1110 on each machine.

<pre>
    dcmd server --port 1060                                         \
        --zkPort 1110 --zkData LocalDirectory                       \
        --zk="server1:1110,server2:1110,server3:1110"
</pre>

This command is run on each of the machines "server1", "server2" and "server3"

If for testing, multiple servers are run on one machine, the zookeeper and
delta server ports must be different across all machines.
