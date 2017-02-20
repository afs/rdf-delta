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

package org.seaborne.delta;

import java.net.BindException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.riot.web.HttpOp;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DataPatchServer;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;

public class Setup {
    interface LinkSetup {
//        public void beforeSuite();
//        public void afterSuite();
        public void beforeClass();
        public void afterClass();
        public void beforeTest();
        
        public void relink();       // Same server, new link.
        public void restart();      // Different server, same state.

        public void afterTest();
        public DeltaLink getLink();
    }
    
    public static class LocalSetup implements LinkSetup {
        protected LocalServer lserver = null;
        protected DeltaLink dlink = null;
        
        @Override
        public void beforeClass() {}

        @Override
        public void afterClass() {}

        @Override
        public DeltaLink getLink() {
            return dlink;
        }

        @Override
        public void beforeTest() {
            lserver = DeltaTestLib.createEmptyTestServer();
            dlink =  DeltaLinkLocal.connect(lserver);
        }
        
        @Override
        public void afterTest() {
            if ( lserver != null )
                LocalServer.release(lserver);
        }

        @Override
        public void relink() {
            Id clientId = dlink.getClientId();
            dlink =  DeltaLinkLocal.connect(lserver);
            if ( clientId != null )
                dlink.register(clientId);
        }
        
        @Override
        public void restart() {
            if ( lserver == null )
                lserver = DeltaTestLib.createEmptyTestServer();
            else {
                LocalServerConfig config = lserver.getConfig() ;
                LocalServer.release(lserver);
                lserver = LocalServer.attach(config);
            }
            relink();
        }
    }

    public static class RemoteSetup implements LinkSetup {

        private static int TEST_PORT=4141;
        
        /** Start a server - this server has no backing local DelaLink
         * which is reset for each test. This enables the server to be reused 
         * (problems starting and stopping the background server
         * synchronous to the tests otherwise).   
         */
        public static DataPatchServer startPatchServer() {
            DataPatchServer dps = DataPatchServer.create(TEST_PORT, null) ;
            try { dps.start(); }
            catch (BindException ex) { throw IOX.exception(ex); }
            return dps;
        }
        
        public static void stopPatchServer(DataPatchServer dps) {
            dps.stop();
            // Clear cached connections.
            resetDefaultHttpClient();
        }
        
        // Local server of the patch server.
        private LocalServer localServer = null;
        private static DataPatchServer server = null;
        private DeltaLink dlink = null;
        
        @Override
        public void beforeClass() {
            if ( server == null )
                server = startPatchServer();
        }
        
        @Override
        public void afterClass() {
            stopPatchServer(server);
            server = null ;
            
        }

        @Override
        public void beforeTest() {
            localServer = DeltaTestLib.createEmptyTestServer();
            DeltaLink localLink = DeltaLinkLocal.connect(localServer);
            server.setEngine(localLink);
            dlink = new DeltaLinkHTTP("http://localhost:"+TEST_PORT+"/");
        }

        @Override
        public void afterTest() {
            LocalServer.release(localServer);
            server.setEngine(null);
            dlink = null;
        }
        
        @Override
        public void relink() {
            Id clientId = dlink.getClientId();
            resetDefaultHttpClient();
            dlink = new DeltaLinkHTTP("http://localhost:"+TEST_PORT+"/");
            if ( clientId != null )
                dlink.register(clientId);
        }
        
        
        @Override
        public void restart() {
            LocalServerConfig config = localServer.getConfig() ;
            LocalServer.release(localServer);
            localServer = LocalServer.attach(config);
            resetDefaultHttpClient();
            DeltaLink localLink = DeltaLinkLocal.connect(localServer);
            server.setEngine(localLink);
            relink();
        }

        @Override
        public DeltaLink getLink() {
            return dlink;
        }

        private static void resetDefaultHttpClient() {
            setHttpClient(HttpOp.createDefaultHttpClient());
        }
        
        /** Set the HttpClient - close the old one if appropriate */
        /*package*/ static void setHttpClient(HttpClient newHttpClient) {
            HttpClient hc = HttpOp.getDefaultHttpClient() ;
            if ( hc instanceof CloseableHttpClient )
                IO.close((CloseableHttpClient)hc) ;
            HttpOp.setDefaultHttpClient(newHttpClient) ;

        }
    }
}
