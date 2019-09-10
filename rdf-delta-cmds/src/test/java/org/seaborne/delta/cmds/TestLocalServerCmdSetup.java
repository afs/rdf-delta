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

package org.seaborne.delta.cmds;

import static org.junit.Assert.assertEquals;

import delta.server.DeltaServerCmd;
import delta.server.DeltaServerConfig;
import delta.server.ServerBuildLib;
import org.apache.jena.atlas.lib.FileOps;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.seaborne.delta.server.Provider;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;

/** Set up a server check its configuration */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TestLocalServerCmdSetup {
    private static String FILEDIR   = "target/FileStore";
    private static String FILEDIR2  = "target/RocksStore";

    public TestLocalServerCmdSetup() { }

    @Before public void before() {
        FileOps.ensureDir(FILEDIR);
        FileOps.clearAll(FILEDIR);
    }

    @AfterClass public static void afterClass() {
        FileOps.clearDirectory(FILEDIR);
        FileOps.deleteSilent(FILEDIR);
    }

    // Tests of configuration.
    @Test public void localServer1_dftLocal() {
        String[] args = {"--base="+FILEDIR};
        LocalServer server = buildLocalServer(args);
        assertEquals(Provider.LOCAL, server.getPatchStore().getProvider().getType());
    }

    @Test public void localServer2_forceFile() {
        String[] args = {"--base="+FILEDIR, "--file"};
        FileOps.ensureDir(FILEDIR);
        LocalServer server = buildLocalServer(args);
        assertEquals(Provider.FILE, server.getPatchStore().getProvider().getType());
    }

    /** The essential steps from DeltaServerCmd/ServerBuildLib to build a {@link LocalServer} */
    private LocalServer buildLocalServer(String[] args) {
        DeltaServerConfig deltaServerConfig = DeltaServerCmd.config(args);
        LocalServerConfig localServerConfig = ServerBuildLib.setupLocalServerConfig(deltaServerConfig);
        LocalServer server = LocalServer.create(localServerConfig);
        return server;
    }
}
