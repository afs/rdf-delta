# Use Cases for RDF Delta

Distributing changes to a dataset has two aspects: the format used to
record changes and the mechanism used to propagate changes.

RDF Delta provides thse into two parts:

* RDF Patch - a format to record changes that is easy to generate and consume.
* RDF Patch Logs - a protocol for distributing change files.

The patch format is separate from the use as a log.

RDF Delta is an implementation of these in patch log server.

RDF Patch files can distributed in many other ways such as
publish-subscribe systems, whether a streaming system or an enterprise
message bus, as well feed mechanisms such as
[Atom](https://tools.ietf.org/html/rfc4287) or
[RSS](https://en.wikipedia.org/wiki/RSS).

## RDF Patch Uses

* Audit
* Incremental backup.

A simple use of RDF Patch is to record the changes made to a dataset.
Once these are reliable recorded, that can be used for tasks such as
auditing changes and incremental backups.

Each change is the addition or deletion of a triple, quad or prefix.
Whether a change is applied once or multiple times, the resulting RDF is
the same.  An RDF graph is a set of triples - add a triple once and there
is one triple in the graph; add it again, and there is still one triple
in the graph.
 
RDF Patch files can be used for backups or to make the same changes to a
different copy of the RDF database so there are two identical copies,
right down to the blank nodes.

## RDF Patch Logs

By collecting RDF patches into a ordered list of changes, 

A log is a sequence of changes that has two operations:

* add a new log item to head of the log
* read log entries

so a log has no branches (theer is only one head of the log at any one
time and only one item can be added to the head to create a new head). A
log is linear list of operations in the time order they were performed.

Given these two operations, 

## High Availability

One important use case provides by RDF Delta is for high availability of
servers.

High availability (HA), also called Fault Tolerance, one or more triple stores 

RDF Delta provides the patch log protocol.

It also provides one impleemnation 

Other implementations are possible to suit the 






## With Logging

## Ways to Distribute

## With Logging nd distrbution

---------------------------------------

* Publishing changes
* In-sync replicas (high availability)
* Incremental backup

Two parts: 
1. It is a log-per-dataset.
1. The log has adds and deletes.

## Publishing changes

## In-sync replicas (high availability)

## Incremental backup

## Analyse changes
??

