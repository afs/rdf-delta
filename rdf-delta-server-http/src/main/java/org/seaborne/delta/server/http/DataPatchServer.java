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

import javax.servlet.Servlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.seaborne.delta.DPConst;
import org.seaborne.delta.Delta;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DPS;

/** A simple packaging of Jetty to provide an embeddable HTTP server that just supports servlets */ 
public class DataPatchServer {
    
    private final boolean loopback = false;
    private final Server server;
    private final ServletHandler handler;
    private final int port;

    public DataPatchServer(int port, DeltaLink engine) {
        DPS.init();
        this.port= port;
        this.server = new Server(port);
        ErrorHandler eh = new HttpErrorHandler();
        eh.setServer(server);
        this.handler = new ServletHandler();
        server.setHandler(handler);
        server.addBean(eh);

        S_Patch patchMgr = new S_Patch(engine);
        // Receive patches
        addServlet("/patch", patchMgr);

        // Return patches
        addServlet("/"+DPConst.EP_Fetch, new S_FetchId(engine));
        addServlet("/"+DPConst.EP_Patch+"/*", new S_Fetch(engine));

        // Other
        addServlet("/rpc", new S_DRPC(engine));
        addServlet("/restart", new S_Restart());
        addServlet("/ping", new S_Ping());
    }
    
    public void addServlet(String path, Servlet servlet) {
        handler.addServletWithMapping(new ServletHolder(servlet), path);
    }
    
    public void start() throws BindException {
        try {
            server.start();
            //server.dumpStdErr();
            Delta.DELTA_LOG.info("DeltaServer starting");
        }
        catch (BindException ex) {
            throw ex;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void join() {
        try {
            server.join();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
