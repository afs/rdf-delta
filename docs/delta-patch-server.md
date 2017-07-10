---
layout: doc
title: Delta Patch Server
nav_text: Delta Patch Server
section: 4
---
@@

Short names.
Versions
API

----

### Control

In addition there are operations to find out about the patch log:
These are provided as a simple JSON API for easy use from javascript
applications.

The request is a JSON document of the form:

```
{
   "op" : "..."
   "token" : "..."
   "arg" : { ... }
}
```

`"op"` gives the operation name, `"arg"` is the operation specific
arguments, and `"token"` the connection registration.  The result is a
JSON value that depends entirely on the operation called.

| Operation                |      |
| ------------------------ | -------------------------- |
| `"list_datasource"`        | List datasource ids        |
| `"list_descriptions"`      | List datasource with details such as the log.|
| `"describe_datasource"`    | Describe a datasource, fixed information only      |
| `"describe_log"`           | Describe a log, including the current latest patch |
| `"create_datasource"`      | Create a log for a dataset |
| `"remove_datasource"`      | Remove a log for a dataset |
| `"ping"`                   | Ping operation for checking reachability of the DPS. |

### Registration and Security


| Operation        |      |
| ---------------- | ---- |
| `"register"`      | Register client                            |
| `"isregistered"`  | Check whether a client is registered       |
| `"deregister"`     | Remove a registration                      |

Registration and allocation of a token (a UUID) gives a control over access.

### API

See the [API description](delta-api.html) for details.

The API is connection-based. A client creates a "link" to Delta Patch
Server and then "connection" when working with a specific dataset and
its patch log.

@@ DeltaClient.
@@ When to sync

```
// Connect to a server.
DeltaLink dLink = DeltaLinkHTTP.connect("http://localhost:1066/");
DeltaClient dClient =

// Connect to a patch log within that server by name.
try ( DeltaConnection dConn = DeltaConnection.connect(zone, clientId, null, null, dLink) ) {
    int version1 = dConn.getRemoteVersionLatest();
    System.out.println("Version = "+version1);

    // Change the dataset
    DatasetGraph dsg = dConn.getDatasetGraph();
    Txn.executeWrite(dsg, ()->{
        dsg.add(quad);
    });

    int version2 = dConn.getRemoteVersionLatest();
    System.out.println("Version = "+version2);
}
```
