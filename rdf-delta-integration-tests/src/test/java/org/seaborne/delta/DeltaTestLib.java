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

import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServers;

public class DeltaTestLib {
    // Static resources area.
    protected static String TDIR = "testing/";
    public static String ServerArea = "target/test/server";

    private static void ensureClear(String area) {
        FileOps.ensureDir(area);
        FileOps.clearAll(area);
    }

    static LocalServer createEmptyTestServerFile() {
        DPS.resetSystem();
        ensureClear(ServerArea);
        LocalServer localServer = LocalServers.createFile(ServerArea);
        return localServer;
    }

    static LocalServer createEmptyTestServerRocks() {
        DPS.resetSystem();
        ensureClear(ServerArea);
        LocalServer localServer = LocalServers.createRDB(ServerArea);
        return localServer;
    }

//
    static Quad freshQuad() {
        return SSE.parseQuad("(_ :s :p '"+DateTimeUtils.nowAsXSDDateTimeString()+"'^^xsd:dateTimeStamp)");
    }
}
