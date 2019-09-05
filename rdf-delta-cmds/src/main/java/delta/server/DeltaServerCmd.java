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

package delta.server;

import static org.seaborne.delta.server.Provider.*;

import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import jena.cmd.ArgDecl;
import jena.cmd.CmdException;
import jena.cmd.CmdLineArgs;
import jena.cmd.TerminationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.web.WebLib;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.cmds.DeltaLogging;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.lib.SystemInfo;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.http.ZkMode;
import org.seaborne.delta.server.s3.InitZkS3;
import org.slf4j.Logger;

/** Command line run the server. */
public class DeltaServerCmd {
    // This class is (mostly) responsible for command line to DeltaServerConfig.

    static { DeltaLogging.setLogging(true); }

    private static Logger LOG = Delta.DELTA_LOG;

    private static ArgDecl argHelp              = new ArgDecl(false, "help", "h");
    private static ArgDecl argVerbose           = new ArgDecl(false, "verbose", "v");
    //private static ArgDecl argVersion    = new ArgDecl(false, "version");
    private static ArgDecl argPort              = new ArgDecl(true, "port");

    private static ArgDecl argBase              = new ArgDecl(true,  "base");
    // Specific choices
    private static ArgDecl argFile              = new ArgDecl(false, "file");
    private static ArgDecl argRocks             = new ArgDecl(false, "rocks", "rocksdb", "rocksDB");

    private static ArgDecl argMem               = new ArgDecl(false, "mem");
    private static ArgDecl argStore             = new ArgDecl(true, "store", "rdb");
    private static ArgDecl argZk                = new ArgDecl(true, "zk");
    private static ArgDecl argZkPort            = new ArgDecl(true, "zkPort", "zkport");
    private static ArgDecl argZkData            = new ArgDecl(true, "zkData", "zkdata");
    private static ArgDecl argZkConf            = new ArgDecl(true, "zkCfg", "zkcfg", "zkConf", "zkconf");

    private static ArgDecl argS3Bucket          = new ArgDecl(true, "s3bucket");
    private static ArgDecl argS3Region          = new ArgDecl(true, "s3region");
    private static ArgDecl argS3KeysFile        = new ArgDecl(true, "s3keys");
    private static ArgDecl argS3KeysProfile     = new ArgDecl(true, "s3profile");
    // Allow alternative endpoints (e.g. a mock S3 store)
    private static ArgDecl argS3Endpoint        = new ArgDecl(true, "s3endpoint");

    private static ArgDecl argJetty             = new ArgDecl(true, "jetty");

//    private static ArgDecl argProvider = new ArgDecl(true, "provider");
//    private static ArgDecl argConf = new ArgDecl(true, "conf", "config");

    // Switch for command line testing to be able to run the server,
    // know it has started on return, and it is not blocking.

    public static void main(String...args) {

        try {
            DeltaServer deltaServer = server(args);

            if ( deltaServer == null ) {
                System.err.println("Failed to run the server");
                System.exit(1);
            }
            // And away we go.
            try {
                FmtLog.info(Delta.DELTA_LOG, "%s %s %s", SystemInfo.systemName(), SystemInfo.version(), SystemInfo.buildDate());
                deltaServer.start();
                deltaServer.join();
                System.exit(0);
            } catch(BindException ex) {
                cmdLineError("Port in use: port=%d", deltaServer.getPort());
            }
        } catch (TerminationException ex) {
            System.exit(ex.returnCode);
        } catch (CmdException ex) {
            System.err.println("Error: "+ex.getMessage());
        }
    }

    public static DeltaServer server(String...args) {
        try {
            DeltaServerConfig deltaServerConfig = config(args);

//            if ( false ){
//                ByteArrayOutputStream out = new ByteArrayOutputStream();
//                DeltaServerConfig.writeJSON(deltaServerConfig, out);
//                ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
//                DeltaServerConfig deltaServerConfig2 = DeltaServerConfig.read(in);
//                boolean b = Objects.equals(deltaServerConfig, deltaServerConfig2);
//                if ( !b ) {
//                    System.err.println("Different round-trip configurations: ");
//                    DeltaServerConfig.writeJSON(deltaServerConfig, System.err);
//                    DeltaServerConfig.writeJSON(deltaServerConfig2, System.err);
//                }
//            }

            DeltaServer deltaServer = ServerBuildLib.build(deltaServerConfig);
            return deltaServer;
        }  catch (DeltaConfigException ex) {
            cmdLineError(ex.getMessage());
            return null;
        }
    }

    public static DeltaServerConfig config(String...args) {
        try {
            DeltaServerConfig deltaServerConfig = processArgs(args);
            return deltaServerConfig;
        } catch (TerminationException ex) {
            throw ex;
        } catch (CmdException ex) {
            cmdLineError(ex.getMessage());
            return null;
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
        cla.add(argFile);
        cla.add(argRocks);

        cla.add(argMem);
        cla.add(argStore);

        cla.add(argZk);
        cla.add(argZkConf);
        cla.add(argZkPort);
        cla.add(argZkData);

        cla.add(argS3Bucket);
        cla.add(argS3Region);
        cla.add(argS3KeysFile);
        cla.add(argS3KeysProfile);
        cla.add(argS3Endpoint);

        //cla.add(argConf);
        cla.process();

        if ( cla.hasPositional() )
            cmdLineWarning("Warning: ignoring positional arguments");

        if ( cla.contains(argHelp) ) {
            System.err.println("Usage: server [--port=NNNN | --jetty=FILE] [--base=DIR] [--mem] [--zk=connectionString [--zkPort=NNN] [--zkData=DIR] ]");
            String msg = StrUtils.strjoinNL
                ("        --port              Port number for the patch server."
                ,"        --jetty=FILE        File name of a jetty.xml configuration file."
                ,"Local database patch server:"
                ,"        --store=DIR         File system directory"
                ,"File based patch server:"
                ,"        --base=DIR          File system directory"
                ,"Simple testing"
                ,"        --mem               Run a single server with in-memory index and patch storage."
                ,"Zookeeper index server:"
                ,"        --zk=               Zookeeper connection string (e.g. \"host1:port1,host2:port2,host3:port3\")"
                ,"   Embedded Zookeeper server"
                ,"        --zkConf=FILE       Zookeeper configuration file. Runs a Zookeeper Server as part of a quorum ensemble. Must be in the connection string."
                ,"        --zkData            Storage for the embedded zookeeper"
                ,"        --zkPort            Port for the embedded Zookeeper."
                ,"   Test Zookeeper"
                ,"        --zk=mem            Run a single Zookeeper without persistent storage."
                ,"   S3 patch storage"
                ,"        --s3bucket          S3 bucket name"
                ,"        --s3region          S3 region"
                ,"        --s3keys            S3 access and secret access keys file (if not set, use default credentials mechanism)"
                ,"        --s3profile         S3 keys file profile"
                ,"        --s3endpoint        S3 alternative endpoint e.g. a store that provides the s3 API"
                );
            System.err.println(msg);
            throw new TerminationException(0);
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
            x++;
            provider = LOCAL;
        }

        if ( cla.contains(argStore) ) {
            x++;
            provider = LOCAL;
        }

        if ( cla.contains(argZk) ) {
            x++;
            if ( cla.contains(argS3Bucket) ) {
                InitZkS3.register();
                provider = ZKS3;
            }
            else
                provider = ZKZK;
        }
        if ( cla.contains(argMem) ) {
            x++;
            provider = MEM;
        }

        if ( x < 1 ) {
            if ( args.length == 0 )
                cmdLineError("No arguments : one of --mem , --store , --base or --zk is required");
            else
                cmdLineError("No provider : one of --mem , --store , --base or --zk is required");
        }
        if ( x > 1 )
            cmdLineError("Exactly one of --mem , --store , --base or --zk is required");

        if ( provider == LOCAL ) {
            // Force choice of local provider.
            if ( cla.contains(argFile) ) provider = FILE;
            if ( cla.contains(argRocks) ) provider = ROCKS;
        }


        DeltaServerConfig serverConfig = new DeltaServerConfig();

        serverConfig.provider = provider;

        // Server.
        serverConfig.serverPort = null;
        serverConfig.jettyConf  = cla.getValue(argJetty);
        if ( serverConfig.jettyConf == null ) {
            int port = chooseServerPort(cla);
            // Check early, otherwise it happens after initializing patch stores and patch logs.
            if ( LibX.isPortInUser(port) )
                cmdLineError("Port %d is in use",port);
            serverConfig.serverPort = port;
        }

        // Providers
        switch(provider) {
            case FILE : {
                String directory = cla.getValue(argBase);
                if ( directory == null )
                    cmdLineError("No such directory given: --base required");
                Path base = Paths.get(directory).toAbsolutePath();
                if ( ! Files.exists(base) )
                    cmdLineError("No such directory: %s",base);
                if ( ! Files.isDirectory(base) )
                    cmdLineError("Exists, but is not a directory: %s",base);
                serverConfig.fileBase = directory;
                break;
            }
            case ROCKS : {
                String directory = cla.getValue(argStore);
                Path base = Paths.get(directory).toAbsolutePath();
                if ( ! Files.exists(base) )
                    cmdLineError("No such directory: %s",base);
                if ( ! Files.isDirectory(base) )
                    cmdLineError("Exists, but is not a directory: %s",base);
                serverConfig.fileBase = directory;
                break;
            }
            case LOCAL : {
                String directory1 = cla.getValue(argBase);
                String directory2 = cla.getValue(argStore);
                if ( directory1 != null && directory2 != null ) {
                    if ( directory1.equals(directory2) )
                        cmdLineWarning("Both --base and --store given with teh same value. Only --store is needed.");
                    else
                        cmdLineError("Both --base=%s and --store=%s given with different values. Only --store is needed.", directory1, directory2);
                }

                String directory = LibX.firstNonNull(directory1, directory2);

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
        if ( connectionString == null )
            return;
        serverConfig.zkMode = ZkMode.EXTERNAL;
        serverConfig.zkPort = null;
        serverConfig.zkData = null;


        // serverConfig.zkMode = ??
        if ( connectionString.equalsIgnoreCase("mem") ) {
            // Memory test mode. No other arguments.
            serverConfig.zkMode = ZkMode.MEM;
            if ( cla.contains(argZkPort) || cla.contains(argZkData) || cla.contains(argZkConf) ) {
                cmdLineWarning("WARNING: Local zookeeper with test memory mode: --zkPort, --zkData and --zkConf ignored");
            }
        } else if ( cla.contains(argZkConf) ) {

            if ( cla.contains(argZkPort) )
                cmdLineWarning("WARNING: Local zookeeper: --zkConf present: ignoring --zkPort");
            if ( cla.contains(argZkData) )
                cmdLineWarning("WARNING: Local zookeeper: --zkConf present: ignoring --zkData");

            serverConfig.zkConf = cla.getValue(argZkConf);
            serverConfig.zkMode = ZkMode.QUORUM;

        } else if ( cla.contains(argZkPort) || cla.contains(argZkData) ) {
            // Check --zkPort and --zkData present together.
            // Setting for a single persistent zookeeper in-process with provided port and data areas.
            if ( ! cla.contains(argZkPort) )
                cmdLineError("No ZooKeeper port: need --zkPort=NNNN with --zkData");
            if ( ! cla.contains(argZkData) )
                cmdLineError("No ZooKeeper data folder: need --zkData=DIR with --zkPort");

            if ( cla.contains(argZkConf) ) {
                cmdLineWarning("WARNING: Local zookeeper: --zkConf not allowed with --zkPort, --zkData");
            }

            serverConfig.zkPort = parseZookeeperPort(cla.getValue(argZkPort));
            serverConfig.zkData = cla.getValue(argZkData);
            serverConfig.zkMode = ZkMode.SINGLE;

            // Make sure port is in the connection string.
            if ( ! connectionString.contains(Integer.toString(serverConfig.zkPort)) )
                cmdLineWarning("WARNING: Local zookeeper not in the connection string. (string=%s, port=%d)", connectionString, serverConfig.zkPort);
        } else {

        }

        FmtLog.debug(LOG,"Connection string: %s", connectionString);
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
        serverConfig.s3CredentialsFile = credentialsFile;

        String credentialsProfile = cla.getValue(argS3KeysProfile);
        serverConfig.s3CredentialsProfile = credentialsProfile;

        String region = cla.getValue(argS3Region);
        if ( StringUtils.isBlank(region) )
            cmdLineError("No S3 region name provided");
        serverConfig.s3Region = region;

        String endpoint = cla.getValue(argS3Endpoint);
        serverConfig.s3Endpoint = endpoint;
    }

    private static int parseZookeeperPort(String portStr) {
        if ( portStr == null )
            cmdLineError("No Zookeeper port number: use --%s", argZkPort.getKeyName());
        try {
            int port = Integer.parseInt(portStr);
            if ( port == 0 )
                return WebLib.choosePort();
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

    private static void cmdLineWarning(String fmt, Object...args) {
        if ( ! fmt.endsWith("\n") )
            fmt = fmt + "\n";
        System.err.printf(fmt, args);
    }

    private static void cmdLineError(String fmt, Object...args) {
        if ( fmt.endsWith("\n") )
            fmt = fmt.trim();
        String msg = String.format(fmt, args);
        throw new CmdException(msg) {
            @Override public Throwable fillInStackTrace() { return this ; }
        };
    }

//    private static void cmdLineError(String fmt, Object...args) {
//        if ( ! fmt.endsWith("\n") )
//            fmt = fmt.trim();
//        String msg = String.format(fmt, args);
//        System.err.printf(msg+"\n", args);
//        System.exit(1);
//    }
}
