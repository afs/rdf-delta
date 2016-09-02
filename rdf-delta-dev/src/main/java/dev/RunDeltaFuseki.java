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

package dev;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.fuseki.FusekiLogging ;
import org.apache.jena.fuseki.cmd.FusekiCmd ;
import org.apache.jena.fuseki.jetty.JettyServerConfig ;
import org.apache.jena.fuseki.server.ServerInitialConfig ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class RunDeltaFuseki {
    static { 
        LogCtl.setLog4j();
        FusekiLogging.allowLoggingReset(false);
    }
    
    static Logger LOG = LoggerFactory.getLogger("Main") ;
    
    public static void main(String[] args) throws Exception {
        String fusekiHome = "/home/afs/Jena/jena-fuseki2/jena-fuseki-core" ;
        String fusekiBase = "/home/afs/tmp/run" ;
        
        System.setProperty("FUSEKI_HOME", fusekiHome) ;
        System.setProperty("FUSEKI_BASE", fusekiBase) ;

        //FusekiEnv.setEnvironment() ;
        FusekiCmd.main("--conf", "/home/afs/ASF/rdf-delta/delta-config.ttl") ;
        
//        // Dev
//        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ; 

        JettyServerConfig jettyServerConfig = new JettyServerConfig() ;
        // Dev
        jettyServerConfig.port = 3030 ;
        jettyServerConfig.contextPath = "/" ;
        jettyServerConfig.jettyConfigFile = null ;
        jettyServerConfig.enableCompression = true ;
        jettyServerConfig.verboseLogging = false ;
        
        ServerInitialConfig config = new ServerInitialConfig() ;
        config.argTemplateFile  = null ;
        // Dev
        config.datasetPath = "/rdf" ;
        config.allowUpdate = false ;
        config.dsg = null ;
        config.fusekiCmdLineConfigFile = "/home/afs/ASF/rdf-delta/delta-config.ttl" ;         // Command line --conf.
        config.fusekiServerConfigFile = null ;          // Calculated config.ttl from run area (if not --conf)
        
        FusekiLogging.setLogging();
        FusekiCmd.runFuseki(config, jettyServerConfig); 
    }
}
