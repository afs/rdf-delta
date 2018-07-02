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

package delta.server;

import static org.seaborne.delta.DeltaOps.verString;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List ;
import java.util.Properties;

import jena.cmd.ArgDecl;
import jena.cmd.CmdLineArgs;
import org.apache.curator.test.TestingServer;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.cmds.dcmd;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer ;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.zk.Zk;
import org.slf4j.Logger;

/** Command line run the server. */ 
public class DeltaServer {
    public static void setLogging() {
        dcmd.setLogging();
    }
    
    static { setLogging(); }
    
    private static Logger LOG = DPS.LOG; 

    private static ArgDecl argHelp     = new ArgDecl(false, "help", "h");
    private static ArgDecl argPort     = new ArgDecl(true, "port");

    private static ArgDecl argBase     = new ArgDecl(true, "base");
    private static ArgDecl argZk       = new ArgDecl(true, "zk");
    private static ArgDecl argMem      = new ArgDecl(false, "mem");
    private static ArgDecl argProvider = new ArgDecl(true, "provider");
//    private static ArgDecl argConf = new ArgDecl(true, "conf", "config");

    public static void main(String...args) {
        try { 
            mainSub(args);
        } catch (Throwable th) {
            th.printStackTrace();
        }
    }

    public static void mainSub(String...args) {
        // ---- Command Line
        CmdLineArgs cla = new CmdLineArgs(args);
        
        cla.add(argHelp);
        cla.add(argPort);
        cla.add(argBase);
        cla.add(argZk);
        cla.add(argMem);
        cla.add(argProvider);
        //cla.add(argConf);
        cla.process();
        
        if ( cla.contains(argHelp) ) {
            System.err.println("Usage: server [--port=NNNN] [--base=DIR] [--zk=connectionString] ");
            System.exit(0);
        }
        
//        String configFile = null;
//        if ( cla.contains(argConf) )
//            configFile = cla.getArg(argConf).getValue();
//        else
//            configFile = getenv(DeltaConst.ENV_CONFIG);
        
        // ---- Local server
        Properties properties = new Properties();
        String providerName = null;
        String envBase = System.getenv(DeltaConst.ENV_BASE);
        
        int x = 0 ;
        if ( cla.contains(argBase) )
            x++ ;
        if ( cla.contains(argZk) )
            x++;
        if ( cla.contains(argMem) )
            x++;
        if ( cla.contains(argProvider) )
            x++;
        if ( envBase != null )
            // File base via and environment variable.
            x++;
        if ( x < 1 ) {
            System.err.println("No provider : one of --mem , --zk, --base or --provider is required"); 
            System.exit(1);
        }
        if ( x > 1 ) {
            System.err.println("Exactly one of --mem , --zk, --base or --provider is required"); 
            System.exit(1);
        }
         
        if ( cla.contains(argBase) || envBase != null ) {
            // File-based provider
            String directory = envBase != null ? envBase : cla.getValue(argBase);
            Path base = Paths.get(directory).toAbsolutePath();
            if ( ! Files.exists(base) ) {
                System.err.println("No such directory: "+base);
                System.exit(1);
            }
            if ( ! Files.isDirectory(base) ) {
                System.err.println("Exists, but is not a directory: "+base);
                System.exit(1);
            }
            properties.setProperty("delta.file", directory);
            providerName = DPS.PatchStoreFileProvider;
        }

        if ( cla.contains(argZk) ) {
            String connectionString = cla.getValue(argZk);
            if ( connectionString.startsWith("here") ) {
                if ( ! connectionString.startsWith("here:") ) {
                    System.err.println("Required --zk=here:DIR wher DIR is teh zookeeper data directory"); 
                    System.exit(1);
                }
                String dir = connectionString.substring("here:".length()); 
                // Run a zookeeper server in this process.
                int port = choosePort();
                Zk.runZookeeperServer(port, dir);
                connectionString = "localhost:"+port;
            } else if ( connectionString.equals("test") ) {
                try {
                    @SuppressWarnings("resource")
                    TestingServer server = new TestingServer();
                    server.start();
                    connectionString = "localhost:" + server.getPort();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }

            properties.setProperty("delta.zk", connectionString);
            providerName = DPS.PatchStoreZkProvider;
        }

        if ( cla.contains(argMem) )
            providerName = DPS.PatchStoreMemProvider;
        
        if ( cla.contains(argProvider) )
            providerName = cla.getValue(argProvider);
        
//        String runtimeArea = cla.contains(argBase) ? cla.getArg(argBase).getValue() : null;
//        if ( runtimeArea == null ) {
//            runtimeArea = getenv(DeltaConst.ENV_BASE);
//            // Default to "."?
//            if ( runtimeArea == null ) {
//                System.err.println("Must use --base or environment variable DELTA_BASE to set the server runtime area.");
//                System.exit(1);
//            }
//        }
//        
//        Path base = Paths.get(runtimeArea).toAbsolutePath();
//        if ( ! Files.exists(base) ) {
//            System.err.println("No such directory: "+base);
//            System.exit(1);
//        }
//        if ( ! Files.isDirectory(base) ) {
//            System.err.println("Exists, but is not a directory: "+base);
//            System.exit(1);
//        }
        
        DPS.init();
        
        LocalServerConfig config = LocalServerConfig.create()
            .setLogProvider(providerName)
            .setProperties(properties)
            .build();
        
        LocalServer server = LocalServer.create(config);
        
        // HTTP Server
        
        int port = chooseServerPort(cla, server);
        DeltaLink link = DeltaLinkLocal.connect(server);

        PatchLogServer dps = PatchLogServer.create(port, link) ;
        FmtLog.info(LOG, "Delta Server port=%d", port);
        
        // Information.
        
        List<DataSource> sources = server.listDataSources();

        if ( sources.isEmpty() )
            FmtLog.info(LOG, "  No data sources");
        else {
            //descriptions.forEach(dsd->FmtLog.info(LOG, "   Data source : %s", dsd));
            // Print nicely.
            sources.sort( (ds1, ds2)-> ds1.getName().compareTo(ds2.getName()) );
            sources.forEach(ds->{
                PatchLogInfo info = ds.getPatchLog().getInfo();
                FmtLog.info(Delta.DELTA_LOG, "  Data source: %s version [%s,%s]", info.getDataSourceDescr(), verString(info.getMinVersion()), verString(info.getMaxVersion()) );
            });
        }

        // And away we go.
        try { 
            dps.start();
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            System.exit(0);
        }
        dps.join();
    }
    
    private static int chooseServerPort(CmdLineArgs cla, LocalServer server) {
        // The port chosen from this ordered list:
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
    
    /** Choose an unused port for a server to listen on */
    private static int choosePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException ex) {
            throw new RuntimeException("Failed to find a port");
        }
    }
}
