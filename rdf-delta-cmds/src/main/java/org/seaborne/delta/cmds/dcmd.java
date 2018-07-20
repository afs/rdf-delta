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
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.LogCtl;

/** Subcommand dispatch.
 *  Usage: "dcmd SUB ARGS...
 */
public class dcmd {

    private static String log4Jsetup = StrUtils.strjoinNL
        ( "## Command default log4j setup"
         
          ,"## Plain output with level, to stderr"
          ,"log4j.appender.jena.plainlevel=org.apache.log4j.ConsoleAppender"
          ,"log4j.appender.jena.plainlevel.target=System.err"
          ,"log4j.appender.jena.plainlevel.layout=org.apache.log4j.PatternLayout"
          ,"log4j.appender.jena.plainlevel.layout.ConversionPattern=%d{HH:mm:ss} %-5p %-15c{1} :: %m%n"

//          , "## Plain output to stdout, unadorned output format"
//          ,"log4j.appender.jena.plain=org.apache.log4j.ConsoleAppender"
//          ,"log4j.appender.jena.plain.target=System.out"
//          ,"log4j.appender.jena.plain.layout=org.apache.log4j.PatternLayout"
//          ,"log4j.appender.jena.plain.layout.ConversionPattern=%m%n"

          ,"## Everything"
          ,"log4j.rootLogger=INFO, jena.plainlevel"
          ,"log4j.logger.org.apache.jena=WARN"
          ,"log4j.logger.org.apache.jena.tdb.loader=INFO"
          ,"log4j.logger.org.eclipse.jetty=WARN"
          ,"log4j.logger.org.apache.zookeeper=WARN"
          ,"log4j.logger.org.apache.curator=WARN"
          ,""
          ,"log4j.logger.org.seaborne.delta=INFO"
          ,"log4j.logger.org.seaborne.patch=INFO"
          ,"log4j.logger.Delta=INFO"
          ,""
          ,"## Parser output"
          ,"log4j.additivity.org.apache.jena.riot=false"
          ,"log4j.logger.org.apache.jena.riot=INFO, jena.plainlevel"
         ) ;

    public static void setLogging() {
        LogCtl.setCmdLogging(log4Jsetup);
    }
    
    static { setLogging(); }

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
            case "append" :
            case "add" :
                cmdExec = "addpatch";
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
            case "addpatch":    addpatch.main(argsSub); break;
            case "getpatch":    getpatch.main(argsSub); break;
            case "rdf2patch":   rdf2patch.main(argsSub); break;
            case "patch2rdf":   patch2rdf.main(argsSub); break;
            case "patchserver": 
                delta.server.DeltaServer.main(argsSub); break;
            default:
                System.err.println("Failed to find a command match for '"+cmd+"'");
        }
    }
}