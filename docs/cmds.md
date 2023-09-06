---
layout: doc
title: Delta - Command Line Tools
nav_text: Command line tools
section: 6
---

The RDF Delta distribution contains a command line utilities for working with
patch logs and patch files.

Each sub-command provides "--help", eg. `dcmd ls --help`

To setup, set the enviroment variable `DELTA_HOME` to the location of
the unpacked RDF Delta distribution. 

The server jar file also contains the comands

`dcmd --help` lists all commands available.

| Sub command | Long Name | Usage | Action |
| :-----  | :-----        |  |  |
| `ls`    | `list`        | `dcmd ls --server=URL`            | List logs on the server |
| `mk`    | `mklog`       | `dcmd mklog --server=URL NAME`    | Create a new patch log |
| `rm`    | `rmlog`       | `dcmd rmlog --server=URL NAME`    | Delete a patch log |
| | | | |
| `get`   | `getpatch`    | `dcmd get --server=URL id`        | Get a patch |
| `append`   | `addpatch`    | `dcmd get --server=URL id FILE`   | Add a patch |
| | | | |
| `r2p`   | `rdf2patch`   | `dcmd r2p FILE`                   | Convert RDF to an addition patch |
| `p2r`   | `patch2rdf`   | `dcmd p2r --data QUADS FILE ...`  | Apply patches to RDF data |
| | | | |
| `server` | `patchserver` | `dcmd server --base DIR`         | Run a patch log server |

## `ls` 

List the patch logs on the server.

```
dcmd ls --server URL
dcmd list --server URL
```

## `mklog`

Create new patch log, supplying a name and, optionally, a URI for the log.

`--server URL` is the URL of the patch log server.

```bash
dcmd mklog --server URL [--uri=uri] NAME ....
```

## `rmlog`

Make the patch log invisible to the API.

This operation does not permanently delete the log, it marks it as
unavailable. Manual cleanup is required to delete the state of the log.

A new patch log of the same name can not created until the old log state
has been removed.

## `get`

Fetch a patch from a patch log with name 'NAME'.

```bash
dcmd get --server URL --dsrc NAME id
```

## `add`

Append a patch to the named log.

```bash
dcmd get --server URL --dsrc NAME PATCH ...
```

## `r2p`

Local operation to convert an RDF file into a patch consisting of `A` or
`PA` record (add triple or quad, add prefix).

```bash
    dcmd rdf2patch FILE
```

## `p2r`

Apply one or more RDF patch files to RDF data.

If the data is given by `--data QUADS`, then the file "QUADS" is read in,
the patches in `FILE ...` are applied and the resultign RDFdataset written out to stdout.

If the `--desc ASSEMBLER` arguemnt is given, the assembler is called to
construct an RDF dataset which may be a persistent one. The patches in
`FILE ...` are applied. The result is not written out.

```bash
dcmd patch2rdf [--data QUADS | --desc ASSEMBLER ] FILE ...
```

## `parse`

Parse a patch file - this tests the synatx for validity.

```bash
dcmd parse FILE ...
```

## `patchserver`

Run a patch server on this machine. The default port is 1066. 
Basic use for a single patch server running with patch log store
in directory `DIR`:

```bash
dcmd patchserver [--port=NNNN] --store=DIR
```

The [full description of RDF Delta Server operation](delta-server) gives more details.
