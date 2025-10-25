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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.seaborne.delta.cmds.CmdTestLib.cmdq;

import org.apache.jena.atlas.lib.FileOps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLog;

/** Set up a server and perform tests */

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TestCmdServer {
    private static String FILEDIR   = "target/FileStore";

    public TestCmdServer() { }

    @BeforeClass static public void beforeClass() {
        FileOps.ensureDir(FILEDIR);
    }

    @Before public void before() {
        FileOps.ensureDir(FILEDIR);
    }

    // Not parameterized - easier to run individual cases.

    @Test public void server2_mem() {
        String[] args = {"--mem"};
        serverAndVerify(args);
    }

    @Test public void server4_fileStore() {
        String[] args = {"--base="+FILEDIR};
        serverAndVerify(args);
    }

    @Test public void server5_jettyConfig() {
        String[] args = {"--jetty=testing/jetty.xml", "--mem"};
        String serverURL = CmdTestLib.server(args);
        // jetty.xml has port 1070
        serverURL = serverURL.replaceAll(":\\d+", ":1070");
        verifyServer(serverURL);
    }

    public static void serverAndVerify(String[] args) {
        String serverURL = CmdTestLib.server(args);
        verifyServer(serverURL);
    }

    private static void verifyServer(String URL) {
        DeltaLink dLink = DeltaLinkHTTP.connect(URL);

        cmdq("mk", "--server="+URL, "ABC");

        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName("ABC");
        assertNotNull(dsd);

        cmdq("append", "--server="+URL, "--log=ABC", "testing/data.rdfp");
        DeltaLog log = new DeltaLog(dLink, dsd.getId());
        PatchLogInfo info = log.info();
        assertEquals(1, log.getCurrentVersion().value());

        cmdq("fetch", "--server="+URL, "--log=ABC", "1");

        cmdq("rm", "--server="+URL, "ABC");
        DataSourceDescription dsd1 = dLink.getDataSourceDescriptionByName("ABC");
        assertNull(dsd1);
    }
}
