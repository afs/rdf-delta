---
layout: doc
title: RDF Patch Logs
nav_text: RDF Change Logs
section: 3
---

A patch log is a sequence of patches which 

The operations on a log are to append a new patch to the log and to
fetch patched from the log.

Once added to a log, a patch is never changed.

### Header Information

Certain metadata is required in all patches.

There must be exactly one `id` header row with a globally unique URI.

There is at most one `prev` header row which identifies the previous in
a path log.,





To organise the patches:

ID
PREV

Short names. URIs.

For example, 

Versions: convenience.

## Naming

http://.../{shortName}/
          /{shortName}/init -- "version 0" but dataset vs patch.
          /{shortName}/current --  "version MaxInt"
          /{shortName}/patch/{version} -- all digits.
          /{shortName}/patch/{id} -- UUID string - has "-"

## Ranges

From an ID to the latest.

## HTTP Operations
