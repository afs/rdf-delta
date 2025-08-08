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
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.junit.Test;

public class TestDeltaServerConfig {

    @Test public void server_config_mem_1() {
        test("--mem");
    }

    @Test public void server_config_mem_2() {
        test("--port=1064", "--mem");
    }

    @Test public void server_config_mem_3() {
        test("--jetty=jetty.xml", "--mem");
    }

    private DeltaServerConfig test(String...args) {
        DeltaServerConfig c = DeltaServerCmd.processArgs(args);
        roundTrip(c);
        return c;
    }

    private void roundTrip(DeltaServerConfig c) {
        JsonObject obj = c.asJSON();
        DeltaServerConfig c2 = DeltaServerConfig.create(obj);
        if ( ! c.equals(c2) ) {
            JSON.write(obj);
            JSON.write(c2.asJSON());
        }
        assertEquals(c, c2);
    }
}
