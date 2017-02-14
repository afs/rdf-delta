# RDF Delta

RDF Delta is a system for replication of RDF Triplestores. Changes made
at one store are propagated to other places - other stores, or to an
archive area, or to a log or ....

## Scenarios

### Data staging

### Master-slave

### Logging changes


## Technology


It consists of:
* A syntax to express changes
* A simple protocol for distributing changes

In addition, there is a partial mapping to SPQRQL Update so that any triplestore can be updated.

## Implementation

* An implementation
  * A general purpose server
  * A client for Apache Jena



Handling blank nodes.

RDF Delta supports changes involving blank nodes. Triplestores uses some kind of internal system id to idenitify blanks. RDF Delta encourages using this to create a URI that can be trsnamitted and used to reconstruct the same internal system id.

For further discussion, see the section [3.5 Replacing Blank Nodes with IRIs](https://www.w3.org/TR/rdf11-concepts/#section-skolemization) in ["RDF 1.1 Concepts and Abstract Syntax"](https://www.w3.org/TR/rdf11-concepts/)

Apache Jena ARQ uses the form ``<_:...>`` for such URIs.  It's parsers systematically trun such URI black into blank nodes.




## RDF Patch v2:
* adds units of update (transaction boundaries)
* add prefix updates
* simplfies the format

## SPARQL Udpate

Limitations
