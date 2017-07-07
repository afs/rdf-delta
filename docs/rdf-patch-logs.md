---
layout: doc
title: RDF Patch Logs
nav_text: RDF Change Logs
section: 3
---

A patch log is a sequence of patches where one patch is to be applied after another.

The operations that can be performed on a a log are to append a new patch to
the log and to fetch patches from the log. A patch in a log is never changed once appended.

## Identifying Patches

A patch in a log need to have an id given by a meta header record in the patch.

```
H id <uuid:fcd707ad-29ed-4c5d-aaf9-f2dcbc5020c6> .
```

The id can be any global unique identifier but using UUID URIs is desirable.
Patches can be moved and copied so an id that is independent of location is needed.

In addition, the patch needs to identify the previous patch, and it must match the id
of the current latest log entry. If there is a mismatch between the actual latest entry
and the one named in the patch, the patch is rejected.

```
H prev <uuid:66e1c2ea-e9a5-4bbf-a3c3-f41b9ce1cafa> .
```

A missing `prev` header means it is the first patch in a log and the log would need to be empty
when then patch is appended.

This ensures that the log is a linear sequence and that when a patch is appended
to the log, it has been done knowing the state of the log.
A patch log can be rebuilt from a set of patches by reading the patch headers and
rebuilding the one-way linked list from latest to earliest that the `prev` headers
define.

This is an [optimistic concurrency control scheme](https://en.wikipedia.org/wiki/Optimistic_concurrency_control_) .

## Applying Patches

A patch log can be be used as a triple store journal of "re-do" operations.

Given a known starting state of an RDF dataset (such as empty), applying the log one
or more times will result in the same new state of the RDF dataset.

Patches can be applied multiple times because an RDF graph is a set of triples -
adding twice or deleting twice has the same effect of adding once or deleting once.

## Header Information

There must be exactly one `id` header row with a globally unique URI.

There is at most one `prev` header row which identifies the previous in
a patch log. It must match the current latest log entry at the point a patch is
appended to a log.

If there is no `prev` header row, the log must be empty when the patch is appended.

## Organising logs

A log has a short name, which is must start with a letter, number or "_" and
only contain letters, numbers, "."  "_" and "-".

It is only unique within the server for the patch log.

In addition, patch logs maintain a version number, an integer, so that it is
possible to go from one patch to the next, later, patch.  Note that client of the patch log
should not assume version numbers are consecutive, although this is desirable in an
implementation, and may occasionally have gaps where there is no patch for a given number.
(a patch may have failed to be appended and the server allocates versions in a way that
is not instantaneous with appending a patch).

## Naming

@@ describe the HTTP URL naming for a patch log.  
@@ To be defined.
```
http://.../{shortName}/
          /{shortName}/init -- "version 0" but dataset vs patch.
          /{shortName}/current --  "version MaxInt"
          /{shortName}/patch/{version} -- all digits.
          /{shortName}/patch/{id} -- UUID string - has "-"
```

## HTTP Operations

|Operation                              | Effect        |
| ---------                             | ------        |
| `POST http://.../{shortName}/`        | Append to log |
| `GET http://.../{shortName}/{id}`     | Get a patch   |
| `GET http://.../{shortName}/version`  | Get a patch   |
