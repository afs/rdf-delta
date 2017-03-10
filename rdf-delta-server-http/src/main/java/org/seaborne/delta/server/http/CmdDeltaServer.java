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

package org.seaborne.delta.server.http;

import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import jena.cmd.ArgDecl;
import jena.cmd.CmdLineArgs;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Delta;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.slf4j.Logger;

/** Command line run the server. */ 
public class CmdDeltaServer {
    public static void setLogging() {
        //LogCtl.setLog4j();
        LogCtl.setJavaLogging();
    }
    
    static { setLogging(); }
    
    private static Logger LOG = DPS.LOG; 

    private static ArgDecl argPort = new ArgDecl(true, "port");
    private static ArgDecl argBase = new ArgDecl(true, "base");
    private static ArgDecl argConf = new ArgDecl(true, "conf", "config");

    public static void main(String...args) {
        // ---- Command Line
        CmdLineArgs cla = new CmdLineArgs(args);
        
        cla.add(argPort);
        cla.add(argBase);
        cla.add(argConf);
        cla.process();
        
        String configFile = null;
        if ( cla.contains(argConf) )
            configFile = cla.getArg(argConf).getValue();
        else
            configFile = getenv(DeltaConst.ENV_CONFIG);
        
        // ---- Environment
        String runtimeArea = cla.contains(argBase) ? cla.getArg(argBase).getValue() : null;
        if ( runtimeArea == null ) {
            runtimeArea = getenv(DeltaConst.ENV_BASE);
            // Default to "."?
            if ( runtimeArea == null ) {
                System.err.println("Must use --base or environment variable DELTA_BASE to set the server runtime area.");
                System.exit(1);
            }
        }
        
        Path base = Paths.get(runtimeArea).toAbsolutePath();
        if ( ! Files.exists(base) ) {
            System.err.println("No such directory: "+base);
            System.exit(1);
        }
        if ( ! Files.isDirectory(base) ) {
            System.err.println("Exists, but is not a directory: "+base);
            System.exit(1);
        }

        //String installArea = getenv(HOME);

        // Logging.
        
        // Server setup - find registered sources.
//        Path sources = base.resolve(DPNames.SOURCES).toAbsolutePath();
//        Path patches = base.resolve(DPNames.PATCHES).toAbsolutePath();
//        DataSource.formatSourceArea(sourceArea, patchesArea);
//        DataSource.cleanSourceArea(sourceArea, patchesArea);
        Location baseArea = IOX.asLocation(base);
        
        if ( configFile == null )
            configFile = baseArea.getPath(DeltaConst.SERVER_CONFIG);
        
        FmtLog.info(LOG, "Delta Server configuration=%s", baseArea);
        LocalServer server = LocalServer.attach(baseArea, configFile);
        int port = choosePort(cla, server);
        DeltaLink link = DeltaLinkLocal.connect(server);
        DataPatchServer dps = DataPatchServer.create(port, link) ;
        // And away we go.
        FmtLog.info(LOG, "START: Delta Server port=%d, base=%s", port, base.toString());
        try { 
            dps.start();
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            System.exit(0);
        }
        //dps.join();
    }

    private static int choosePort(CmdLineArgs cla, LocalServer server) {
        // The port choosen from this ordered list:
        //   Command line
        //   Environment variable
        //   Server config file
        //   Default.
        
        int port = -1;
        String portStr = null;
        if ( cla.contains(argPort) ) {
            portStr = cla.getArg(argPort).getValue();
        } else {
            portStr = getenv(DeltaConst.ENV_PORT);
        }
        
        if ( portStr != null ) {
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException ex) {
                System.err.println("Failed to parse the port number: "+portStr);
                System.exit(1);
            }
        }
        
        if ( port == -1 )
            port = DeltaConst.PORT;
        
        return port ;

    }
    
    /** Check/format the server area. */
    private static void ensure_setup(Path sources, Path patches) {
        FileOps.ensureDir(sources.toString());
        FileOps.ensureDir(patches.toString());
        // Setup - need better registration based on scan-find.
    }

    /** Look for the setting for an environment variable.
     * This can be in:
     * <ul>
     * <li>The OS environment.
     * <li>Java defined system property.
     * </ul> 
     * The search is in that order.
     */
    public static String getenv(String name) {
        String x = System.getenv(name) ;
        if ( x == null )
            x = System.getProperty(name) ;
        return x ;
    }
}
