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

import org.apache.jena.atlas.web.WebLib;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.http.DeltaServer;
import org.seaborne.delta.server.local.LocalServers;

public class TestLogLockMem extends AbstractTestLogLock {

    private static int PORT = WebLib.choosePort();
    private static DeltaServer deltaServer;
    private static DeltaLink dLink;

    @BeforeClass static public void before() {
        deltaServer = DeltaServer.create(PORT, LocalServers.configMem());
        // And away we go.
        try {
            deltaServer.start();
        } catch(BindException ex) {
            System.err.printf("Port in use: port=%d", deltaServer.getPort());
            throw new RuntimeException("Failed to start", ex);
        }
        dLink = DeltaLinkHTTP.connect("http://localhost:"+PORT+"/");
    }

    @AfterClass static public void after() {
        deltaServer.stop();
    }

    @Override
    protected DeltaLink getDLink() {
        return dLink;
    }
}
