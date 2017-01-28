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

import org.apache.jena.atlas.io.IO;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DataPatchServer;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;

public class Setup {
    interface LinkSetup {
        public void beforeClass();
        public void afterClass();
        public void beforeTest();
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
            dlink =  DeltaLinkLocal.create(lserver);
        }
        
        @Override
        public void afterTest() {
            if ( lserver != null )
                LocalServer.release(lserver);
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
            DataPatchServer dps = new DataPatchServer(TEST_PORT, null) ;
            try { dps.start(); }
            catch (BindException e) {
                e.printStackTrace();
                IO.exception(e);
            }
            return dps;
        }
        
        public static void stopPatchServer(DataPatchServer dps) {
            dps.stop();
        }
        
        // Local server of the patch server.
        private LocalServer localServer = null;
        private DataPatchServer server = null;
        private DeltaLink link = null;
        
        @Override
        public void beforeClass() { 
            server = startPatchServer();
        }
        
        @Override
        public void afterClass() {
            stopPatchServer(server);
        }

        @Override
        public void beforeTest() {
            localServer = DeltaTestLib.createEmptyTestServer();
            DeltaLink localLink = DeltaLinkLocal.create(localServer);
            server.setEngine(localLink);
            link = new DeltaLinkHTTP("http://localhost:"+TEST_PORT+"/");
        }

        @Override
        public void afterTest() {
            LocalServer.release(localServer);
            server.setEngine(null);
            link = null;
        }

        @Override
        public DeltaLink getLink() {
            return link;
        }

    }
}
