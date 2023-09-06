---
layout: doc
title: Delta - Synchronizing RDF Datasets
nav_text: Publishing RDF Changes
section: 1
---
Delta is a system for keeping copies of an RDF Dataset up to date.

It uses RDF Patch as a general purpose way to record changes to
an [RDF Dataset](https://www.w3.org/TR/rdf11-concepts/#section-dataset)
then provides a log of all changes to that dataset.

The technology of RDF patch and the Patch log can also be used to
publish changes to the dataset.

The code here provides a patch server, protocol and API for
synchronizing datasets more aimed at synchronizing replicas of datasets
in webapp servers.

## RDF Patch

RDF patch is a format for recording changes to an
[RDF Dataset](https://www.w3.org/TR/rdf11-concepts/#section-dataset).
It provides a way to handle blank nodes so that datasets can keep the
system ids typically used for blank nodes internally in step.

Patches are organised into a [log](rdf-patch-logs.html) making a general purpose way
to record changes and be able to fetch them later to apply to copies to being
them up-to-date.

This is an evolution of the original RDF Patch described in
"[RDF Patch &ndash; Describing Changes to an RDF Dataset](https://afs.github.io/rdf-delta/rdf-patch.html)".
This new version is changed in the light of experience of using the
format. It is not compatible with the previous version.  The changes
simplify the design by remove unnecessary features, add support for
managing namespace prefixes and provide a header for the patch for
necessary metadata.

For more details, see "[RDF Patch](rdf-patch.html)".

### Example RDF Patch Log Entry

This example ensures certain prefixes are in the dataset and adds some
basic triples for a new subclass of `<http://example/SUPER_CLASS>`.

```
H  id   <uuid:016f5292-2b49-11b2-80fe-6057182f557b> .
H  prev <uuid:dd85b5f7-9965-42df-866c-d456bd1409de> .
TX .
PA "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#" .
PA "owl" "http://www.w3.org/2002/07/owl#" .
PA "rdfs" "http://www.w3.org/2000/01/rdf-schema#" .
A  <http://example/SubClass> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .
A  <http://example/SubClass> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example/SUPER_CLASS> .
A  <http://example/SubClass> <http://www.w3.org/2000/01/rdf-schema#label> "SubClass" .
TC .
```

`H` is a header, `TX` , `TC` the start and end of changes, `PA` means "Prefix Add",
and `A` means "data add".

Applying this patch when it has already been applied results in the same
dataset state. Applying patches in the order the patches were
created is idempotent.

### RSS and Atom

The nature of a patch log means it is suitable to publish changes using
RSS or Atom.  Systems across the web can then choose to update their
copy of a dataset.  More metadata about a match would need to be added
to the feed but RDF Patch and the patch log can provide the building
blocks for publishing changes to a dataset.

Delta does not currently provide an RSS or Atom feed.

## Patch Log

The operations on a patch log are to be able to add a new patch to the
end of the log, or to read any patch or subsequence of patches from the
log. The log is maintained using the patch headers  `id` and `prev`.

The `id` is a globally unique identifier for this patch; it can also be
used to give a global version to the dataset that the patch creates.

The `prev` header field indicates which previous state of the
dataset this patch applies to.

Delta enforces the rule that to append to the log, the patch must
include in the previous field the id of the current latest patch in the log.
If the wrong id is given, or it is missing and the log is not empty,
the attempt to append the patch is rejected. This stops two independent applications
from logging a change to the same dataset version at the same time.
While quite a strict way to guarantee the order, for systems of only a
few machines, this simple mechanism is clearer, signals rejecting
patches at the earliest possible moment and means that the log can be
reconstructed just by reading the headers of patches.

This means the log of sequence of patches with no branches.  This is
only one way to use RDF Patch.  In other scenarios, a tree of changes
might be allowed, or other meta data might describe when the patch
should be applied; it may not be related to earlier patches and can be
used separately.  Delta is not trying to support all possible uses of
RDF Patch as a change record.

## Patch Server

The Delta Patch Server (DPS) provides access to logs for a number of datasets,
appending a patch and retrieving a sequence.

With the initial state of the dataset and the log of patches, it is
possible to create later versions of the dataset because a log is
sequence of patches in the order the changes happened.

### Datasets and Patch Logs

Each dataset being managed has a short name similar in usage to a
database name in SQL systems. This name is only identifying within the
patch server.

The log, any initial data for setup and the state is arranged into "data sources",
each of which has a short name as well as a globally unique identifier.

This design facilities the need to have patch log servers for development cycle such as
"development", "staging" and "production".

### Access

The patch log is managed by two URLs; one to POST a request to append to
the log, one to fetch patches using GET.  They can be the same or
different - having different URLs can make implementing security easier
in some situations depending on the access control framework.

In the example below the same URL "patch-log" is used.

Patches are sent to the server by POST:

```
POST http://host/.../{NAME}/
```
with a `Location:` in the response and retrieved by
```
GET http://host/.../{NAME}/patch/{id}
```
