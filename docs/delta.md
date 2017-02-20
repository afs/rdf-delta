# Delta : synchronized RDF Datasets

Delta is a system to keep copies of RDF Datasets up to date.

Delta uses RDF Patch as a general purpose way to record changes to
an [RDF Dataset](https://www.w3.org/TR/rdf11-concepts/#section-dataset)
then provides a log of all changes to that dataset.

The technology of RDF patch and the Patch log can also be used to
publish changes to the dataset. 

The code here provides a patch server, protocol and API for
syncrhonizing datasets more aimed at syncrhonizing replicas of datasets
in webapp servers.

## RDF Patch

This section is a brief outline of RDF Patch. 

RDF Patch is a general purpose way to record changes to an [RDF
Dataset](https://www.w3.org/TR/rdf11-concepts/#section-dataset).
It provides a way to handle blank nodes so that datasets can keep the
system ids typically used for blank nodes in step.

See also:

* A longer "[Introduction to RDF Patch](rdf-patch-intro.md)"
* "[RDF Patch Specification](rdf-patch.md)"

This is an evolution of the original RDF Patch described in
"[RDF Patch &ndash; Describing Changes to an RDF Dataset](https://afs.github.io/rdf-patch/)".

This new version is changed in the light of experience of using the
format. It is incompatible with the previous version.  The changes
simplify the design by remove unnecessary features, add support for
managing namespace prefixes and provide a header for the patch for
necessary metadata.

### Brief Example

This example ensures ceratin prefixes are in the dataset and adds some
basic triples for a new subclass of `<http://example/SUPER_CLASS>`.

```
H id       <uuid:016f5292-2b49-11b2-80fe-6057182f557b> .
H previous <uuid:dd85b5f7-9965-42df-866c-d456bd1409de> .
TB .
PA "rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#" .
PA "owl" "http://www.w3.org/2002/07/owl#" .
PA "rdfs" "http://www.w3.org/2000/01/rdf-schema#" .
QA _ <http://example/SubClass> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .
QA _ <http://example/SubClass> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example/SUPER_CLASS> .
QA _ <http://example/SubClass> <http://www.w3.org/2000/01/rdf-schema#label> "SubClass" .
TC .
```

'H` is a header, `TB` , `TC` the start and end of changes, `PA` meanas "Prefix Add",
`QA` is "Quad add" and "_" means default graph.

Applying this patch when it has alreayd been applied results in the same
dataset state. Applying patches in the order the patches were
created is idempotent.

### RSS and Atom

The nature of a patch log means it is suitable to publish changes using
RSS or Atom.  Systems across the web can then choose to update their
copy of a dataset.  More metadata about a match would need to be added
to the feed. but RDF Patch and the patch log can provide the building
blocks for publishing changes to a dataset.

Delta does not currently provide an RSS or Atom feed currently.

## Patch Log

The operations on a patch log are to be able to add a new patch to the
end of the log, or to read any patch or subsequence of patches from the
log. The log is maintained using the patch headers  `id` and `previous`.

The `id` is a globally unique identifier for this patch; it can also be
used to give a global version to the dataset that the patch creates.

The `previous` header field indicates which previous state of the
dataset this patch applies to.

Delta enforces the rule that to append to the log, the patch must
include in the previous field the id of the patch that this new patch
applies to.  If the wrong id is given, or it is missing, the attempt to
append the patch is rejected. This stops two independent applications
from logging a change to the same dataset version at the same time.
While quite a strict way to guaranttee the order, for systems of only a
few machines, this simple mechanism is clearer, signals rejecting
patches at the earliest possible moment and means that the log can be
reconstructed just by reading the headers of patches. 

This means the log of sequence of patches with no branches.  This is
only one way to use RDF Patch.  In other scenarios, a tree of changes
might be allowed, or other meta data might describe when the patch
should be applied; it may not be related to earlier patches and can be
used separately.  Delta is not trying to support all possible uses of
RDF Patch as a change record.

Delta also gives a numeric version number to each patch.  This makes it
easier to work with.  Version numbers are allocated when the patch
server stgarts up.  They may change over time, for example, if a log is
consolidated into a shorter one.

## Patch Server

The Delta patch server (DPS) provides access to logs for a number of datasets,
appending a patch and retriveing a sequence.

@@ "Log Server"

With the initial state of the dataset and the log of patches, it is
possible to create later versions of the dataset because a log is
sequence of patches in the order the changes happened.

### Datasets and Patch Logs

Each dataset being managed has a short name similar in usage to a
database name in SQL systems. This name is only identifying within the
patch server.

Datsets are grouped into "zones" to reflect the need to have development,
staging and deployment systems for the same data.

### Access

The patch log is managed by two URLs; one to POST request to append ot
the log, one to fetch patches using GET.  They can be the same or
different - having different URLs can make implementing security easier
in some situations depending on the access control framework.

In the example below the same URL "patch-log" is used.

Patchs are sent to the server by POST:

    POST http://host/.../NAME/patch-log

and retrieved by

    GET http://host/.../NAME/patch-log?id=...

@@ Todo - range of patches

### Control

In addition there are operations to find out about the patch log:

| Operation | |
| --------- | ---- |
| List datasets and logs | |
| Describe a log | |
| REGISTER | | 
| ISREGISTERED | |
| LIST_DS | |
| DESCR_DS | |
| EPOCH | |
| DEREGISTER | |
| CREATE_DS | |
| REMOVE_DS  | |

These are provides as a simple JSON API to easy use from javascript
applications.

The request is a JSOn document of the form:

```
{
   "op" : "..."
   "token" : "..."
   "arg" : { ... }
}
```

`"op"` gives the operation bname:

@@ _DS => _LOG?

| Operation     | |
| ------------- | ---- |
| LIST_DS       | List datasets and logs |
| DESCR_DS      | Describe a log |
| VERSION       | Latest version number of a log |
| CREATE_DS     | Create a log for a dataset |
| REMOVE_DS     | Remove a log for a dataset |
| REGISTER      | Register client |
| ISREGISTERED  | |
| DEREGISTER    | |

See the [protocol description](delta-protocol.md).

### Registration and Security

### API

See the [API description](delta-api.md) for details.

The API is connection-based. A client creates a "link" to Delta Patch
Server and then "connection" when working with a specific dataset and
its patch log.

```
// Connect to a server.
DeltaLink dLink = DeltaLinkHTTP.connect("http://localhost:1066/");

// Connect to a patch log within that server by name.
try ( DeltaConnection dConn = DeltaConnection.connect("DataName", ... , dLink) ) {
    int version = dConn.getRemoteVersionLatest();
    System.out.println("Version = "+version);
}

```
