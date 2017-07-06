# RDF Patch Specification

This page defines RDF Patch. 

## Structure

The text format for an RDF Patch is N-Triples-like: it is a series of lines, each line ends
with a `.` (DOT).

A line has an operartion code, then some number of items depending on 
the operation.

| Operation |                   |
| --------- | ----------------- |
| `H`                  | Header |
| `TX`<br/>`TC`<br/>`TA`     | Change block: transactions    |
| `PA`<br/>`PD`<br/>         | Change: Prefix add and delete |
| `A`<br/>`D`                | Change: Add and delete triples and quads |

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

A binary version based on [RDF Thrift](http://afs.github.io/rdf-thrift/) will be provided
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

A change is an add or delete of a quad or a prefix.

#### Prefixes

Prefixes do not apply to the data of the patch. They are
changes to the data the patch is applied to.

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
