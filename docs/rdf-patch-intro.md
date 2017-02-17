# Introduction to RDF Patch 

RDF Patch is a general purpose way to record changes to an [RDF
Dataset](https://www.w3.org/TR/rdf11-concepts/#section-dataset).
It provides a way to handle blank nodes so that datasets can keep the
system ids typically used for blank nodes in step.

## Use cases

See [use_cases.md](use_cases.md) (Work-in-progress).

* Synchronizing chnages on dataset relicas 
* Publishing updates to a dataset
* Keeping an incremental backup

This page describes RDF Patch. There is a [full description is RDF Patch
v2](rdf-patch.md).

| Operation |                   |
| --------- | ----------------- |
| `H`                  | Header |
| `TX`, `TC`, `TA`     | Transactions |
| `PA`, `PD`, `BA`    | Prefix and base |
| `QA`, `QD`           | Quad add and delete |

## Structure

The text format for an RDF Patch is N-Triples-like: it is a series of lines, each line ends
with a `.` (DOT).

The general structure is a header (possibly empty) and a sequence of
blocks recording changes. Each change block is a transaction.

```
header
TB
Quad and prefix changes
TC or TA
```

Multiple transaction blocks are allowed for multiple sets of changes in one
patch.

A binary version base on [RDF Thrift](http://afs.github.io/rdf-thrift/) will be provided
sometime.  Parsing binary compared to text for N-triples achieves a x3-x4 increase in
throughput.

### Header

The header provides for basic information about patch. It is a series of
(key, value) pairs.

It is better to put complex metadata in a separate file and link to it
from the header but certain information, is best kept with the patch. An example
used by Delta is to keep the identifer of the global version id of the dataset
so that patches are applied in the right order.

Header format:
```
H word RDFTerm .
```
where `word` is a string in quotes, or an unquoted string (no spaces, starts with a letter,
same as a prefix without the colon).

The header is ended by the first `TX` or the end of the patch.

### Transactions

```
TX .
TC .
```

These delimit a block of quad and prefix changes.

Abort, `TA` is provided so that changes can be streamed, not obliging the
application to buffer change and wait to confirm the action is
committed.

Transactions should be applied atomically when a patch is applied.

### Changes

A change is an add or delete of a quad or a prefix. In addition, a base
URI can be give.

#### Prefixes

Neither prefixed nor base URI apply to the data of the patch. They are
chnages to the data the patch is applied to.

The prefix name is without the trailing colon. It can be given as a
quoted or unquoted string.

```
PA rdf <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
```

#### Quads

Changes to the RDF datasets

@@


### Blank nodes

@@
