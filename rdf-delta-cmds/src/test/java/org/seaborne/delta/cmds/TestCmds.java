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

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLog;

/** Set up a server and perform tests of commands on it. */

public class TestCmds {
    
    private String serverURL;
    private DeltaLink dLink;

    public TestCmds() { }
    
    @Before public void before() {
        int port = LibX.choosePort(); 
        serverURL = CmdTestLib.server("--mem");
        dLink = DeltaLinkHTTP.connect(serverURL);
    }
    
    @Test public void cmd_mk() {
        String LOG_NAME = "ABC_1";
        cmdq("mk", "--server="+serverURL, LOG_NAME);
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(LOG_NAME);
        assertNotNull(dsd);
        assertEquals(LOG_NAME, dsd.getName());
        List<DataSourceDescription> list = dLink.listDescriptions();
        assertEquals(1, list.size());
        
        DeltaLog log = new DeltaLog(dLink, dsd.getId());
        PatchLogInfo info = log.info();
        assertEquals(0, log.getCurrentVersion().value());
    }
        
    @Test public void cmd_mk_rm() {
        String LOG_NAME = "ABC_2";
        cmdq("mk", "--server="+serverURL, LOG_NAME);
        cmdq("rm", "--server="+serverURL, LOG_NAME);
        DataSourceDescription dsd1 = dLink.getDataSourceDescriptionByName(LOG_NAME);
        assertNull(dsd1);
    }
}
