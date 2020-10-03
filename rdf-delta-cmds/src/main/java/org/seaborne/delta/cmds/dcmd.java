/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.cmds;

import java.util.Arrays;

import jena.cmd.CmdException;
import org.apache.jena.Jena;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.fuseki.main.cmds.FusekiMainCmd;
import org.seaborne.delta.lib.SystemInfo;

/** Subcommand dispatch.
 *  Usage: "dcmd SUB ARGS...
 */
public class dcmd {
    static { DeltaLogging.setLogging(true); }

    public static class RDF_Delta {
        // For org.apache.jena.atlas.lib.Version
        static public final String        NAME              = "RDF Delta";
        static public final String        VERSION           = SystemInfo.version();
        static public final String        BUILD_DATE        = SystemInfo.buildDate();
    }

    private static void version() {
//        Metadata system = new Metadata();
//        system.addMetadata("org/seaborne/delta/delta-properties.xml");
//        system.addMetadata("org/apache/jena/jena-properties.xml");
        // Need rewriting! Put back "name".
        // No reflection foo.
        org.apache.jena.atlas.lib.Version version = new org.apache.jena.atlas.lib.Version();
        version.addClass(RDF_Delta.class);
        version.addClass(Jena.class) ;
        version.print(IndentedWriter.stdout);
    }

    public static void main(String...args) {
        if ( args.length == 0 ) {
            System.err.println("Usage: dcmd SUB ARGS...");
            throw new CmdException("Usage: dcmd SUB ARGS...");
        }

        String cmd = args[0];
        String[] argsSub = Arrays.copyOfRange(args, 1, args.length);
        String cmdExec = cmd;

        // Help
        switch (cmdExec) {
            case "help" :
            case "-h" :
            case "-help" :
            case "--help" :
                System.err.println("Commands: server, ls, mk, rm, list, get, add, parse, r2p, p2r");
                return;
            case "version":
            case "--version":
            case "-version":
                version();
                System.exit(0);
        }

        // Map to full name.
        switch (cmdExec) {
            case "server" : cmdExec = "patchserver"; break;
            case "ping":    cmdExec = "ping"; break;

            case "mk" : cmdExec = "mklog"; break;
            case "ls" : cmdExec = "list";  break;
            case "cp" : cmdExec = "cplog"; break;
            case "mv" : cmdExec = "mvlog"; break;
            case "rm" : cmdExec = "rmlog"; break;

            case "appendpatch" :
            case "add" :
                cmdExec = "append";
                break;

            case "get" :
            case "fetch" :
                cmdExec = "getpatch";
                break;

            case "cat":
                cmdExec = "catpatch";
                break;

            case "p": case "parse":
                cmdExec = "parse";
                break;
            case "r2p":
                cmdExec = "rdf2patch";
                break;
            case "p2r":
                cmdExec = "patch2rdf";
                break;
            case "p2u":
                cmdExec = "patch2update";
                break;
        }

        // Execute sub-command
        switch (cmdExec) {
            case "mklog":           mklog.main(argsSub); break;
            case "mvlog":           mvlog.main(argsSub); break;
            case "cplog":           cplog.main(argsSub); break;
            case "rmlog":           rmlog.main(argsSub); break;
            case "list":            list.main(argsSub); break;

            case "append":          append.main(argsSub); break;
            case "getpatch":        getpatch.main(argsSub); break;

            case "catpatch":        catpatch.main(argsSub); break;

            case "rdf2patch":       rdf2patch.main(argsSub); break;
            case "patch2rdf":       patch2rdf.main(argsSub); break;
            case "patch2update":    patch2update.main(argsSub); break;
            case "ping":            pingserver.main(argsSub); break;
            case "parse":           patchparse.main(argsSub); break;

            case "patchserver":
                delta.server.DeltaServerCmd.main(argsSub); break;
            case "monitor" :        monitor.main(argsSub); break;

            case "fuseki":
                FusekiMainCmd.main(argsSub);
                break;
            default:
                System.err.println("Failed to find a command match for '"+cmd+"'");
        }
    }
}