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
* Local file system using plain files for patch storage
* Single server, in-memory patch logs.

When the server starts with a local mode, the storage choice (plain file, or
local database) for existing patch logs is preserved. The server setup influences
the storage for patch logs when they are created.

The recommended deployment choices are using RocksDB storage (`--store`), with
either a filesystem which should be on reliable storage or a backed up
filesystem.

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

## Examples

### Run a single server

This example is for running a single patch log server on the default port (1066)
with a RockDB database for patches. Patch logs are stored in the directory
"Delta" which must exist.

<pre>
    dcmd server --store Delta
</pre>
