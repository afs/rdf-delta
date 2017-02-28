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
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.Servlet;

import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.seaborne.delta.DPConst;
import org.seaborne.delta.Delta;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DPS;

/** A simple packaging of Jetty to provide an embeddable HTTP server that just supports servlets */ 
public class DataPatchServer {
    
    private final boolean loopback = false;
    private final Server server;
    private final int port;
    // Shared across servlets.
    private final AtomicReference<DeltaLink> engineRef;
    
    /** Create a patch log server that uses the given local {@link DeltaLink} for its state. */   
    public static DataPatchServer create(int port, DeltaLink engine) {
        return new DataPatchServer(port, engine);
    }

    private DataPatchServer(int port, DeltaLink engine) {
        DPS.init();
        this.port= port;
        this.server = jettyServer(port, false);
        this.engineRef = new AtomicReference<>(engine);
        
        ServletContextHandler handler = buildServletContext("/");
        
        // Combined name.
        addServlet(handler, "/"+DPConst.EP_PatchLog, new S_PatchLog(this.engineRef));
        
        // Receive patches
        addServlet(handler, "/"+DPConst.EP_Append, new S_Patch(this.engineRef));
        // Return patches
        addServlet(handler, "/"+DPConst.EP_Fetch, new S_Fetch(this.engineRef));

//        // Trailing name.
//        addServlet("/"+DPConst.EP_Fetch+"/*", new S_Fetch(this.engineRef));

        // Other
        addServlet(handler, "/rpc", new S_DRPC(this.engineRef));
        addServlet(handler, "/restart", new S_Restart());
        addServlet(handler, "/ping", new S_Ping());

        server.setHandler(handler);
    }
    
    /** Build a ServletContextHandler. */
    private static ServletContextHandler buildServletContext(String contextPath) {
        if ( contextPath == null || contextPath.isEmpty() )
            contextPath = "/" ;
        else if ( !contextPath.startsWith("/") )
            contextPath = "/" + contextPath ;
        ServletContextHandler context = new ServletContextHandler() ;
        context.setDisplayName("PatchLogServer") ;
        ErrorHandler eh = new HttpErrorHandler();
        context.setErrorHandler(eh) ;
        return context ;
    }
    
    /** Build a Jetty server */
    private static Server jettyServer(int port, boolean loopback) {
        Server server = new Server() ;
        HttpConnectionFactory f1 = new HttpConnectionFactory() ;
        // Some people do try very large operations ... really, should use POST.
        f1.getHttpConfiguration().setRequestHeaderSize(512 * 1024);
        f1.getHttpConfiguration().setOutputBufferSize(5 * 1024 * 1024) ;
        // Do not add "Server: Jetty(....) when not a development system.
        if ( true )
            f1.getHttpConfiguration().setSendServerVersion(false) ;
        ServerConnector connector = new ServerConnector(server, f1) ;
        connector.setPort(port) ;
        server.addConnector(connector);
        if ( loopback )
            connector.setHost("localhost");
        return server ;
    }
    
    /** Internal */
    public void setEngine(DeltaLink engine) {
        engineRef.set(engine);
    }
    
    private void addServlet(ServletContextHandler holder, String path, Servlet servlet) {
        holder.addServlet(new ServletHolder(servlet), path);
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
    
    public void stop() {
        try {
            //Delta.DELTA_LOG.info("DeltaServer stopping");
            server.stop();
            Delta.DELTA_LOG.info("DeltaServer stopped");
        } catch (Exception e) {
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
