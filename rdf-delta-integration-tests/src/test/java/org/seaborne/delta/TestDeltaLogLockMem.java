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

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.web.WebLib;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

public class TestDeltaLogLockMem extends AbstractTestDeltaLogLock {

    // If running independently...
    //static { TC_DeltaIntegration.setForTesting(); }

    private static DeltaServer deltaServer;
    private static DeltaLink dLink;

    @BeforeClass public static void beforeClass() {
        int port = WebLib.choosePort();
        LocalServerConfig localServerConfig = LocalServers.configMem();
        deltaServer = DeltaServer.create(port, localServerConfig);

        //FmtLog.info(Delta.DELTA_LOG, "%s %s %s", SystemInfo.systemName(), SystemInfo.version(), SystemInfo.buildDate());
        try {
            deltaServer.start();
        } catch(BindException ex) {
            IO.exception(ex);
        }

        dLink = DeltaLinkHTTP.connect("http://localhost:"+port+"/");
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName("ABC");
        dsRef = ( dsd != null ) ? dsd.getId() : dLink.newDataSource("ABC", "http://data/ABC");
    }

    @AfterClass public static void afterClass() {
        if ( deltaServer != null )
            deltaServer.stop();
    }

    @Override
    protected DeltaLink getDLink() {
        return dLink;
    }
}
