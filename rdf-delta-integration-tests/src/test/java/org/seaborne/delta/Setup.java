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

package org.seaborne.delta;

import java.net.BindException;

import org.apache.jena.atlas.lib.Creator;
import org.apache.jena.atlas.web.WebLib;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.*;

public class Setup {
    static { DPS.init(); }

    public interface LinkSetup {
//        public void beforeSuite();
//        public void afterSuite();
        public void beforeClass();
        public void afterClass();
        public void beforeTest();
        public void afterTest();

        public void relink();       // Same server, new link.
        public void restart();      // Different server, same state.

        public DeltaLink getLink();
        public DeltaLink createLink();  // New link every time.
        public boolean restartable();
    }

    public static class LocalSetup implements LinkSetup {
        protected LocalServer lserver = null;
        protected DeltaLink dlink = null;
        private final Creator<LocalServer> builder;
        private final boolean restartable;

        private LocalSetup(Creator<LocalServer> builder, boolean restartable) {
            this.builder = builder;
            this.restartable = restartable;
        }

        public static LinkSetup createMem() {
            return new LocalSetup(()->LocalServers.createMem(), false);
        }

        public static LinkSetup createFile() {
            return new LocalSetup(()->DeltaTestLib.createEmptyTestServerFile(), true);
        }

        public static LinkSetup createRocksDB() {
            return new LocalSetup(()->DeltaTestLib.createEmptyTestServerRocks(), true);
        }

        @Override
        public void beforeClass() { DPS.init(); }

        @Override
        public void afterClass() {}

        @Override
        public DeltaLink getLink() {
            return dlink;
        }

        @Override
        public DeltaLink createLink() {
            return DeltaLinkLocal.connect(lserver);
        }

        @Override
        public void beforeTest() {
            lserver = builder.create();
            dlink = createLink();
        }

        @Override
        public void afterTest() {
            if ( lserver != null )
                LocalServer.release(lserver);
        }

        @Override
        public void relink() {
            dlink =  DeltaLinkLocal.connect(lserver);
        }

        @Override
        public void restart() {
            if ( lserver == null )
                lserver = builder.create();
            else {
                LocalServerConfig config = lserver.getConfig() ;
                LocalServer.release(lserver);
                lserver = LocalServer.create(config);
            }
            relink();
        }

        @Override
        public boolean restartable() {
            return restartable;
        }
    }

    public static class RemoteSetup implements LinkSetup {

        // Local server of the patch server.
        private LocalServer localServer = null;
        private DeltaServer server = null;
        private DeltaLink dlink = null;
        private int testPort = -999;

        @Override
        public void beforeClass() {
        }

        @Override
        public void afterClass() {
        }

        @Override
        public void beforeTest() {
            testPort = WebLib.choosePort();
            localServer = DeltaTestLib.createEmptyTestServerRocks();
            DeltaLink localLink = DeltaLinkLocal.connect(localServer);
            server = DeltaServer.create(testPort, localLink);
            try {
                server.start();
            } catch (BindException e) {
                e.printStackTrace();
            }
            dlink = createLink();
        }

        @Override
        public void afterTest() {
            server.stop();
            LocalServer.release(localServer);
            dlink = null;
            testPort = -999;
        }

        @Override
        public void relink() {
            dlink = createLink();
        }


        @Override
        public void restart() {
            server.stop();
            //testPort = LibX.choosePort();
            LocalServerConfig config = localServer.getConfig() ;
            LocalServer.release(localServer);
            localServer = LocalServer.create(config);
            DeltaLink localLink = DeltaLinkLocal.connect(localServer);
            server = DeltaServer.create(testPort, localLink);
            try {
                server.start();
            } catch (BindException e) {
                e.printStackTrace();
            }
            relink();
        }

        @Override
        public boolean restartable() {
            return true;
        }

        @Override
        public DeltaLink getLink() {
            return dlink;
        }

        @Override
        public DeltaLink createLink() {
            return DeltaLinkHTTP.connect("http://localhost:"+testPort+"/");
        }
    }
}
