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

package org.seaborne.delta.server.http;

import java.net.BindException;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.Delta;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.LocalServers;

/**
 * Delta server.
 * This is the {@link PatchLogServer} and all additional servers like ZooKeeper.
 */

public class DeltaServer {
    private final PatchLogServer patchLogServer ;

    /** Create a {@code DeltaServer}. */
    public static DeltaServer create(int port, LocalServerConfig localServerConfig) {
        LocalServer server = LocalServer.create(localServerConfig);
        DeltaLink dLink = DeltaLinkLocal.connect(server);
        return create(port, dLink);
    }

    /** Create a {@code DeltaServer}, with custom Jetty configuration. */
    public static DeltaServer create(String jettyConfig, LocalServerConfig localServerConfig) {
        LocalServer server = LocalServer.create(localServerConfig);
        DeltaLink dLink = DeltaLinkLocal.connect(server);
        return create(jettyConfig, dLink);
    }

    /** Create a {@code DeltaServer} for a file-provider using the {@code base} area.
     *  Convenience operation for:
     *  <pre>
        LocalServer server = LocalServers.createFile(base);
        DeltaLink link = DeltaLinkLocal.connect(server);
        DeltaServer.create(port, link);
     *  </pre>
     */
    public static DeltaServer server(int port, String base) {
        LocalServer server = LocalServers.createFile(base);
        DeltaLink link = DeltaLinkLocal.connect(server);
        return DeltaServer.create(port, link);
    }

    /**
     * Create a patch log server that uses the given {@link DeltaLink},
     * which is usually a {@link DeltaLinkLocal}.
     */
    public static DeltaServer create(int port, DeltaLink engine) {
        PatchLogServer pls = new PatchLogServer(null, port, engine);
        return new DeltaServer(pls);
    }

    /**
     * Create a patch log server that uses the given a Jetty configuation file and a
     * {@link DeltaLink} for its state.
     */
    public static DeltaServer create(String jettyConfig, DeltaLink engine) {
        PatchLogServer pls = new PatchLogServer(jettyConfig, -1, engine);
        return new DeltaServer(pls);
    }

    private DeltaServer(PatchLogServer patchLogServer) {
        this.patchLogServer = patchLogServer;
    }

    public int getPort() {
        return patchLogServer.getPort();
    }

    public DeltaServer start() throws BindException {
        FmtLog.debug(Delta.DELTA_LOG, "Server start: port=%d", getPort());
        patchLogServer.start();
        return this;
    }

    public void stop() {
        patchLogServer.stop();
    }

    public void join() {
        patchLogServer.join();
    }
}
