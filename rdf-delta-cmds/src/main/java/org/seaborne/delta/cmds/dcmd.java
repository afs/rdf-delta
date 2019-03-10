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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import jena.cmd.CmdException;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.fuseki.main.cmds.FusekiMainCmd;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.seaborne.delta.lib.IOX;

/** Subcommand dispatch.
 *  Usage: "dcmd SUB ARGS...
 */
public class dcmd {

    private static String log4j2SysProp = "log4j.configurationFile";

    private static boolean INITIALIZED = false;
    static {
        setLogging();
    }

    public static void setLogging() {

        if ( INITIALIZED )
            return ;
        INITIALIZED = true;

        if ( System.getProperty("log4j.configurationFile") != null ) {
            // Log4j2 configuration set from outside.
            // Leave to log4j2 itself.
            return;
        }

        if ( FileOps.exists("logging.properties") ) {
            System.err.println("RDF Delta 0.7.0 and later uses log4j2 for logging");
            System.err.println("  Found 'logging.properties' (for java.util.logging) - ignored");
        }

        if ( FileOps.exists("log4j.properties") ) {
            System.err.println("RDF Delta 0.7.0 and later uses log4j2 for logging");
            System.err.println("  Found 'log4j.properties' (for log4j1) - ignored");
        }

        if ( FileOps.exists("log4j2.xml") ) {
            // Let Log4j2 initialize normally.
            System.setProperty("log4j.configurationFile", "log4j2.xml");
            return;
        }

        // Stop Jena's cmds initializing logging.
        System.setProperty("log4j.configuration", "off");

        // Initialize log4j2 from a default (in XML non-strict format)
        byte b[] = StrUtils.asUTF8bytes(getDefaultString());
        try (InputStream input = new ByteArrayInputStream(b)) {
            ConfigurationSource source = new ConfigurationSource(input);
            ConfigurationFactory factory = ConfigurationFactory.getInstance();
            Configuration configuration = factory.getConfiguration(null, source);
            Configurator.initialize(configuration);
        }
        catch (IOException ex) {
            IOX.exception(ex);
        }
    }

    // Log4J2, non-strict XML format
    public static String getDefaultString() {

        String defaultLog4j2_xml = String.join("\n"
            ,"<?xml version='1.0' encoding='UTF-8'?>"
            ,"<Configuration status='WARN'>"
            ,"  <Appenders>"
            ,"    <Console name='STDOUT' target='SYSTEM_OUT'>"
            ,"      <PatternLayout pattern='[%d{yyyy-MM-dd HH:mm:ss}] %-10c{1} %-5p %m%n'/>"
            ,"    </Console>"
            ,"  </Appenders>"
            ,"  <Loggers>"
            ,"    <Root level='WARN'>"
            ,"      <AppenderRef ref='STDOUT'/>"
            ,"    </Root>"
            ,"    <Logger name='Delta' level='INFO'/>"
            ,"    <Logger name='org.seaborne.delta' level='INFO'/>"
            ,"    <Logger name='org.apache.jena' level='INFO'/>"
            ,"  </Loggers>"
            ,"</Configuration>"
            );
        return defaultLog4j2_xml;
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
        }

        // Map to full name.
        switch (cmdExec) {
            case "server" :
                cmdExec = "patchserver";
                break;

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
            case "rmlog":           rmlog.main(argsSub); break;
            case "list":            list.main(argsSub); break;

            case "append":          append.main(argsSub); break;
            case "getpatch":        getpatch.main(argsSub); break;

            case "catpatch":        catpatch.main(argsSub); break;

            case "rdf2patch":       rdf2patch.main(argsSub); break;
            case "patch2rdf":       patch2rdf.main(argsSub); break;
            case "patch2update":    patch2update.main(argsSub); break;
            case "parse":           patchparse.main(argsSub); break;
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