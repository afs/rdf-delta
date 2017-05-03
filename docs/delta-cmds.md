# Command Line Tools

(try "--help" for instructions)

## Server

| Command | Purpose |
| patchserver | An RDF delta patch log server |
| mksrc | Create a data source (patch log and initial data) |
| rmsrc | Remove (make inaccessible) a data source |
| rdf2patch | Write out a patch file that adds the data of an RDF file |
| list  | List datsources |

## Running the server

`bin/patchserver`

or

`java -jar rdf-delta-server.jar`

## Ruinning Command Line Admin Tools


The can be run with:

`bin/<name>`

or

`java -cp rdf-delta-server.jar org.seaborne.delta.cmds.<name>`
