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

import static delta.server.Provider.*;
import static delta.server.ZkMode.*;
import static org.seaborne.delta.DeltaOps.verString;

import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List ;
import java.util.Properties;

import jena.cmd.ArgDecl;
import jena.cmd.CmdException;
import jena.cmd.CmdLineArgs;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.FusekiLib;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.cmds.dcmd;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.PatchLogServer ;
import org.seaborne.delta.server.local.*;
import org.seaborne.delta.server.local.patchstores.file.PatchStoreProviderFile;
import org.seaborne.delta.server.local.patchstores.mem.PatchStoreProviderMem;
import org.seaborne.delta.server.local.patchstores.zk.PatchStoreProviderZk;
import org.seaborne.delta.server.s3.PatchStoreProviderZkS3;
import org.seaborne.delta.server.s3.S3;
import org.seaborne.delta.server.system.DeltaSystem;
import org.seaborne.delta.zk.ZkS;
import org.seaborne.delta.zk.ZooServer;
import org.slf4j.Logger;
/** Command line run the server. */ 
public class DeltaServer {
    public static void setLogging() {
        dcmd.setLogging();
    }
    
    //static { setLogging(); }
    static { LogCtl.setJavaLogging(); }
    
    private static Logger LOG = DPS.LOG; 
    
    private static ArgDecl argHelp     = new ArgDecl(false, "help", "h");
    private static ArgDecl argVerbose  = new ArgDecl(false, "verbose", "v");
    //private static ArgDecl argVersion  = new ArgDecl(false, "version");
    private static ArgDecl argPort     = new ArgDecl(true, "port");

    private static ArgDecl argBase     = new ArgDecl(true, "base");
    private static ArgDecl argMem      = new ArgDecl(false, "mem");
    private static ArgDecl argZk       = new ArgDecl(true, "zk");
    private static ArgDecl argZkPort   = new ArgDecl(true, "zkPort", "zkport");
    private static ArgDecl argZkData   = new ArgDecl(true, "zkData", "zkdata");
    
    private static ArgDecl argS3Bucket    = new ArgDecl(true, "s3bucket");
    private static ArgDecl argS3Region    = new ArgDecl(true, "s3region");
    private static ArgDecl argS3KeysFile  = new ArgDecl(true, "s3keys");
    //private static ArgDecl s3Prefix  = new ArgDecl(true, "s3keys");
    
    private static ArgDecl argJetty    = new ArgDecl(true, "jetty");
    //private static ArgDecl argProvider = new ArgDecl(true, "provider");
//    private static ArgDecl argConf = new ArgDecl(true, "conf", "config");

    public static void main(String...args) {
        DeltaSystem.init();
        try {
            DeltaServerConfig deltaServerConfig = processArgs(args);
            // Run ZooKeepers and Delta Patch Server.
            PatchLogServer dps = run(deltaServerConfig);
            //dps.join();
        } catch (CmdException ex) {
            System.err.println(ex.getMessage());
            return;
        } catch (Throwable th) {
            th.printStackTrace();
       }
    }

    public static DeltaServerConfig processArgs(String...args) {
        // ---- Command Line
        CmdLineArgs cla = new CmdLineArgs(args);
        
        cla.add(argHelp);
        //cla.add(argVerbose);
        cla.add(argPort);
        cla.add(argJetty);
        
        cla.add(argBase);
        
        cla.add(argMem);
        
        cla.add(argZk);
        cla.add(argZkPort);
        cla.add(argZkData);
        
        cla.add(argS3Bucket);
        cla.add(argS3Region);
        cla.add(argS3KeysFile);
        
        //cla.add(argConf);
        cla.process();
        
        if ( cla.contains(argHelp) ) {
            System.err.println("Usage: server [--port=NNNN | --jetty=FILE] [--base=DIR] [--mem] [--zk=connectionString [--zkPort=NNN] [--zkData=DIR] ]");
            String msg = StrUtils.strjoinNL
                ("    --port              Port number for the patch server."
                ,"    --jetty=FILE        File name of a jetty.xml configuration file."
                ,"    File based patch server:"
                ,"    --base=DIR          File system directory"
                ,"    Simple testing"
                ,"    --mem               Run a single server with in-memory index and patch storage." 
                ,"    Zookeeper index server:"
                ,"      External zookeeper ensemble"
                ,"    --zk=               Zookeeper connection string (e.g. \"host1:port1,host2:port2,host3:port3\")"
                ,"      Embedded Zookeeper server"
                ,"    --zk=local,...      Zookeeper connection string; 'local' replaced by the embedded server as 'localhost:port'"
                ,"    --zkData            Storage for the embedded zookeeper"
                ,"    --zkPort            Port for the embedded Zookeeper"
                ,"    Test Zookeeper"
                ,"    --zk=mem            Run a single Zookeeper without persistent storage."
                ,"    S3 patch storage"
                ,"    --s3bucket          S3 bucket name"
                ,"    --s3keys            S3 access and secrey access keys file (if not default credentials mechanism)"
                ,"    --s3region          S3 region"
                );
            System.err.println(msg);
            return null;
        }
        
//        String configFile = null;
//        if ( cla.contains(argConf) )
//            configFile = cla.getArg(argConf).getValue();
//        else
//            configFile = getenv(DeltaConst.ENV_CONFIG);
        
        // ---- Local server provider choices.
        
        Provider provider = UNSET;
        int x = 0 ;
        if ( cla.contains(argBase) ) {
            x++ ; provider = FILE;
        }            
        if ( cla.contains(argZk) ) {
            x++ ; 
            if ( cla.contains(argS3Bucket) )
                provider = Provider.ZKS3;
            else
                provider = Provider.ZKZK;
        }            
        if ( cla.contains(argMem) ) {
            x++; provider = Provider.MEM;
        }
        if ( x < 1 )
            cmdLineError("No provider : one of --mem , --zk, --base or --provider is required"); 
        if ( x > 1 )
            cmdLineError("Exactly one of --mem , --zk, --base or --provider is required"); 
        
        DeltaServerConfig serverConfig = new DeltaServerConfig();
        
        serverConfig.provider = provider;
        
        // Server.
        serverConfig.serverPort = null;
        serverConfig.jettyConf  = cla.getValue(argJetty);
        if ( serverConfig.jettyConf == null )
            serverConfig.serverPort = chooseServerPort(cla);
        
        // Providers
        switch(provider) {
            case FILE : {
                String directory = cla.getValue(argBase);
                Path base = Paths.get(directory).toAbsolutePath();
                if ( ! Files.exists(base) )
                    cmdLineError("No such directory: %s",base);
                if ( ! Files.isDirectory(base) )
                    cmdLineError("Exists, but is not a directory: %s",base);
                serverConfig.fileBase = directory;
                break;
            }
            case MEM : {
                // No configuration.
                break;
            }
            case ZKZK : {
                // Complicated - put in a static  
                zookeeperArgs(cla, serverConfig);
                break;
            }
            case ZKS3 : {
                zookeeperArgs(cla, serverConfig);
                s3Args(cla, serverConfig);
                break;
            }
            default : {
                throw new NotImplemented("Provider not recognized: "+provider);
            }
        }
        
        return serverConfig;
    }

    
    private static void zookeeperArgs(CmdLineArgs cla , DeltaServerConfig serverConfig) {
        String connectionString = cla.getValue(argZk);
        String zkServers[] = connectionString.split(",");
        int idx = 0 ;
        String zk1 = zkServers[idx];

        if ( zk1.equals("local") ) {
            // Run a persistent ZooKeeper instance in this process.
            if ( ! cla.contains(argZkData) )
                cmdLineError("Need to provide the ZooKeeper data area directory with --zkData when using '--zk=local'");
            serverConfig.zkData =  cla.getValue(argZkData);

            // Run a zookeeper server in this process.
            String zkPortStr = cla.getValue(argZkPort);
            if ( zkPortStr == null ) 
                cmdLineError("Need to specify the Zookeeper port with --zkPort when using '--zk=local'");
            int zkPort = parseZookeeperPort(zkPortStr);
            zk1 = "localhost:"+zkPort;
            zkServers[idx] = zk1;
            // Rebuild the connectionString.
            connectionString =  String.join(",",zkServers);
            serverConfig.zkMode = ZkMode.EMBEDDED;
            serverConfig.zkPort = zkPort;
            LOG.info("Connection string: "+connectionString);
            if ( ! connectionString.contains(Integer.toString(zkPort)) )
                cmdLineWarning("WARNING: Local zookeeper not in the connection string. (string=%s, port=%d)", connectionString, zkPort);
        } else if ( connectionString.equalsIgnoreCase("mem") ) {
            serverConfig.zkMode = ZkMode.MEM;
        } else {
            if ( cla.contains(argZkPort) || cla.contains(argZkData) ) {
                // Setting for a persistent zookeeper in-process with provided port and data areas.
                // This instance can be contacted by other ZooKeeper servers. 
                if ( ! cla.contains(argZkPort) )
                    cmdLineError("No ZooKeeper port: need --zkPort=NNNN with --zkData");
                if ( ! cla.contains(argZkData) )
                    cmdLineError("No ZooKeeper data folder: need --zkData=DIR with --zkPort");
                serverConfig.zkPort = parseZookeeperPort(cla.getValue(argZkPort));
                serverConfig.zkData = cla.getValue(argZkData);
            }
            serverConfig.zkMode = EXTERNAL;
        }
        serverConfig.zkConnectionString = connectionString;
    }

    private static void s3Args(CmdLineArgs cla , DeltaServerConfig serverConfig) {
        String bucketName = cla.getValue(argS3Bucket);
        if ( StringUtils.isBlank(bucketName) )
            cmdLineError("No S3 bucket name provided");
        serverConfig.s3BucketName = bucketName;
        
        String credentialsFile = cla.getValue(argS3KeysFile);
        if ( credentialsFile != null && credentialsFile.isEmpty() )
            cmdLineError("Empty S3 credentials file");
        serverConfig.s3Credentials = credentialsFile;

        String region = cla.getValue(argS3Region);
        if ( region != null && region.isEmpty() )
            cmdLineError("Empty S3 region name");
        serverConfig.s3Region = region;
    }
    
    private static PatchLogServer run(DeltaServerConfig config) {
        DPS.init();
        
        // Verbose.
        
        // Fix up PatchStoreProviders
        PatchStoreMgr.reset();
        PatchStoreProvider psp;
        LocalServerConfig localServerConfig;
        String providerLabel = "";
        
        switch (config.provider) {
            case FILE :
                psp = installProvider(new PatchStoreProviderFile());
                localServerConfig = LocalServers.configFile(config.fileBase);
                providerLabel = "file["+config.fileBase+"]";
                break;
            case MEM :
                psp = installProvider(new PatchStoreProviderMem());
                localServerConfig = LocalServers.configMem();
                providerLabel = "mem";
                break;
            case ZKZK :
                psp = installProvider(new PatchStoreProviderZk());
                localServerConfig = setupZookeeper(config);
                providerLabel = "zookeeper";
                break;
            case ZKS3 :
                psp = installProvider(new PatchStoreProviderZkS3());
                localServerConfig = setupZookeeper(config);
                localServerConfig = setupS3(config, localServerConfig);
                providerLabel = "zookeeper+s3";
            default :
                throw new IllegalArgumentException("Unrecognized provider: "+config.provider);
        }
        //LOG.debug("Provider: "+psp.getProviderName());
        LOG.info("Provider: "+providerLabel);
        
        Properties properties = new Properties();
        if ( ! properties.isEmpty() )
            localServerConfig = LocalServerConfig.create(localServerConfig).setProperties(properties).build();
    
        LocalServer server = LocalServer.create(localServerConfig);
        
        // HTTP Server
        
        DeltaLink link = DeltaLinkLocal.connect(server);
    
        PatchLogServer dps;
        if ( config.jettyConf != null ) {
            FmtLog.info(LOG, "Delta Server jetty config=%s", config.jettyConf);
            dps = PatchLogServer.create(config.jettyConf, link);
        } else {
            FmtLog.info(LOG, "Delta Server port=%d", config.serverPort);
            dps = PatchLogServer.create(config.serverPort, link);
        }
        
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
            //ex.printStackTrace(System.out);
            if ( config.serverPort != null )
                cmdLineError("Port in use: port=%d", config.serverPort);
            else
                cmdLineError("Port in use: port=%s (defined in '%s')", dps.getPort(), config.jettyConf);
        }
        return dps;
    }

    private static LocalServerConfig setupZookeeper(DeltaServerConfig config) {
        // Allocate zookeeper
        switch(config.zkMode) {
            case NONE : throw new InternalErrorException();
            case EMBEDDED :
                ZooServer zs = ZkS.runZookeeperServer(config.zkPort, config.zkData);
                zs.start();
                break;
            case EXTERNAL :
                break;
            case MEM :
                config.zkConnectionString = ZkJVM.startZooJVM();
                break;
            default :
                break;
        }
        if ( config.zkConnectionString == null ) 
            cmdLineError("No connection string for ZooKeeper");
        LocalServerConfig localServerConfig = LocalServers.configZk(config.zkConnectionString);
        return localServerConfig;
    }

    private static LocalServerConfig setupS3(DeltaServerConfig config, LocalServerConfig localServerConfig) {
        return 
            S3.configZkS3(config.zkConnectionString ,
                          config.s3BucketName,
                          config.s3Region); 
    }

    private static PatchStoreProvider installProvider(PatchStoreProvider psp) {
        PatchStoreMgr.registerShortName(psp.getShortName(), psp.getProviderName());
        PatchStoreMgr.register(psp);
        return psp;
    }
    
    private static int parseZookeeperPort(String portStr) {
        if ( portStr == null )
            cmdLineError("No Zookeeper port number: use --%s", argZkPort.getKeyName());
        try {
            int port = Integer.parseInt(portStr);
            if ( port == 0 )
                return FusekiLib.choosePort(); 
            if ( port < 0 )
                throw new NumberFormatException();
            return port;
        } catch (NumberFormatException ex) {
            cmdLineError("Failed to parse the Zookeeper port number: %s", portStr);
            return -1;
        }
    }
    
    /** Choose a port number or return null */ 
    private static Integer chooseServerPort(CmdLineArgs cla) {
        // The port chosen from this ordered list:
        //   Command line
        //   Environment variable
        //   Default.
        
        int port = -1;
        String portStr = null;
        if ( cla.contains(argPort) )
            portStr = cla.getArg(argPort).getValue();
        else
            // Compatibility
            portStr = getenv(DeltaConst.ENV_PORT);
        
        if ( portStr == null )
            return DeltaConst.PORT;
            //cmdLineError("No server port given: use --%s", argPort.getKeyName());
        
        try {
            port = Integer.parseInt(portStr);
            if ( port <= 0 )
                throw new NumberFormatException();
            return port;
        } catch (NumberFormatException ex) {
            cmdLineError("Failed to parse the port number: %s", portStr);
            return null;
        }
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
    private static String getenv(String name) {
        String x = System.getenv(name) ;
        if ( x == null )
            x = System.getProperty(name) ;
        return x ;
    }
    
    /** */
    private static String runSimpleZookeeper() {
        try {
            @SuppressWarnings("resource")
            TestingServer server = new TestingServer();
            server.start();
            String connectionString = "localhost:" + server.getPort();
            return connectionString;
        }
        catch (Exception ex) {
            throw new CmdException("Zookeeper TestingServer", ex);
        }
    }
//    /** Choose an unused port for a server to listen on */
//    private static int choosePort() {
//        try (ServerSocket s = new ServerSocket(0)) {
//            return s.getLocalPort();
//        } catch (IOException ex) {
//            throw new RuntimeException("Failed to find a port");
//        }
//    }
    
    private static void cmdLineWarning(String fmt, Object...args) {
        if ( ! fmt.endsWith("\n") )
            fmt = fmt + "\n";
        System.err.printf(fmt, args);
    }

    private static void cmdLineError(String fmt, Object...args) {
        if ( fmt.endsWith("\n") )
            fmt = fmt.trim();
        String msg = String.format(fmt, args);
        throw new CmdException(msg);
    }
}
