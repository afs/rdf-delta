/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.cmds;

import java.util.Arrays;

import jena.cmd.CmdException;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.main.cmds.FusekiMainCmd;

/** Subcommand dispatch.
 *  Usage: "dcmd SUB ARGS...
 */
public class dcmd {

    private static Object lock = new Object();
    static {
        setLogging();
    }

    public static void setLogging() {
        if ( System.getProperty("java.util.logging.configuration") != null )
            return;
        synchronized(lock) {
            if ( System.getProperty("java.util.logging.configuration") != null )
                return;
            if ( systemPropertySet("log4j.configuration") )
                // Not used.
                // Leave to CmdMain -> LogCtl.setCmdLogging()
                return;
            // Stop Jena initializing in CmdMain -> LogCtl.setCmdLogging()
            System.setProperty("log4j.configuration", "off");
            LogCtl.setJavaLogging();
            // [Jena 3.9.0] Can be dropped.
            System.setProperty("java.util.logging.configuration", "set");
        }
    }

    private static boolean systemPropertySet(String string) {
        return System.getProperty(string) != null ;
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
                System.err.println("Commands: server, ls, mk, rm, list, get, add, parse, path, r2p, p2r");
                return;
        }

        // Map to full name.
        switch (cmdExec) {
            case "appendpatch" :
            case "add" :
                cmdExec = "append";
                break;
            case "mk" :
                cmdExec = "mklog";
                break;
            case "ls" :
                cmdExec = "list";
                break;
            case "rm" :
                cmdExec = "rmlog";
                break;
            case "get" :
            case "fetch" :
                cmdExec = "getpatch";
                break;
            case "server" :
                cmdExec = "patchserver";
                break;
        }

        // Execute sub-command
        switch (cmdExec) {
            case "mklog":       mklog.main(argsSub); break;
            case "rmlog":       rmlog.main(argsSub); break;
            case "list":        list.main(argsSub); break;
            case "append":      append.main(argsSub); break;
            case "getpatch":    getpatch.main(argsSub); break;
            case "rdf2patch":   rdf2patch.main(argsSub); break;
            case "patch2rdf":   patch2rdf.main(argsSub); break;
            case "patchserver":
                delta.server.DeltaServerCmd.main(argsSub); break;
            case "fuseki":
                FusekiMainCmd.main(argsSub);
                break;
            default:
                System.err.println("Failed to find a command match for '"+cmd+"'");
        }
    }
}