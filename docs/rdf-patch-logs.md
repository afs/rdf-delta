---
layout: doc
title: RDF Patch Logs
nav_text: RDF Change Logs
section: 3
---
# RDF Patch Logs

A patch log is a sequence of patches which 

The oeprations on a log are to append a new patch to the log and to
fetch patched from the log.

Once added to a log, a patch is never changed.

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
