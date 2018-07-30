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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;

import javax.servlet.Filter;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.fuseki.jetty.FusekiErrorHandler1;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.riot.Lang;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.DeltaLinkLocal;
import org.seaborne.delta.server.local.LocalServer;
import org.seaborne.delta.server.local.LocalServers;
import org.slf4j.Logger;

/**
 * An HTTP-based server providing patch logs and the admin functions.
 * <p>
 * Implemented as a packaging of Jetty with the necessary servlets for Delta.
 */
public class PatchLogServer {
    
    private static Logger LOG = Delta.DELTA_SERVER_LOG;
    private final boolean loopback = false;
    private final Server server;
    private final int port;
    private final String jettyConfigFile;
    // Shared across servlets.
    private final DeltaLink deltaLink;
    
    /** Create a {@code PatchLogServer}
     * @param port
     * @param base 
     */
    public static PatchLogServer server(int port, String base) {
        LocalServer server = LocalServers.createFile(base);
        DeltaLink link = DeltaLinkLocal.connect(server);
        return PatchLogServer.create(port, link);
    }
    
    /** Create a patch log server that uses the given local {@link DeltaLink} for its state. */
    public static PatchLogServer create(int port, DeltaLink engine) {
        return new PatchLogServer(null, port, engine);
    }

    /** Create a patch log server that uses the given local {@link DeltaLink} for its state. */
    public static PatchLogServer create(String jettyConfig, DeltaLink engine) {
        return new PatchLogServer(jettyConfig, -1, engine);
    }

    private PatchLogServer(String jettyConfig, int port, DeltaLink dLink) {
        DPS.init();
        
        // Either ... or ...
        this.jettyConfigFile = jettyConfig;

        if ( jettyConfigFile != null ) {
            server = jettyServer(jettyConfigFile);
            this.port = ((ServerConnector)server.getConnectors()[0]).getPort();
        } else {
            server = jettyServer(port, false);
            this.port = port; 
        }

        this.deltaLink = dLink;
        ServletContextHandler handler = buildServletContext("/");
        
        HttpServlet servletRDFPatchLog = new S_Log(dLink);
        HttpServlet servletPing = new S_Ping();
        //HttpServlet servlet404 = new ServletHandler.Default404Servlet();
        
        // Filter - this catches RDF Patch Log requests. 
        addFilter(handler, "/*", new F_PatchFilter(dLink,
                                                   (req, resp)->servletRDFPatchLog.service(req, resp),
                                                   (req, resp)->servletPing.service(req, resp)
                                                   ));
        
        // Other
        addServlet(handler, "/"+DeltaConst.EP_RPC, new S_DRPC(this.deltaLink));
        //addServlet(handler, "/restart", new S_Restart());
        
        addServlet(handler, "/"+DeltaConst.EP_Ping, new S_Ping());  //-- See also the "ping" DRPC.
        
        // Initial data. "/init-data?datasource=..."
        addServlet(handler, "/"+DeltaConst.EP_InitData, new S_Data(this.deltaLink));

        
        // ---- A default servlet at the end of the chain.
//        // -- Jetty default, including static content. 
//        DefaultServlet servletContent = new DefaultServlet();
//        ServletHolder servletHolder = new ServletHolder(servletContent);
//        //servletHolder.setInitParameter("resourceBase", "somewhere");
//        handler.addServlet(servletHolder, "/*");
        
        // -- 404 catch all.
        HttpServlet servlet404 = new Servlet404();
        addServlet(handler, "/*", servlet404);
        // One line error message
        handler.setErrorHandler(new FusekiErrorHandler1());
        // Wire up.
        server.setHandler(handler);
    }
    
    static class Servlet404 extends HttpServlet {
        @Override
        protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            ServletOps.errorNotFound("Not found");
            //resp.sendError(HttpSC.NOT_FOUND_404, "Not found");
        }
    }
    
    /** Build a ServletContextHandler. */
    private static ServletContextHandler buildServletContext(String contextPath) {
        if ( contextPath == null || contextPath.isEmpty() )
            contextPath = "/";
        else if ( !contextPath.startsWith("/") )
            contextPath = "/" + contextPath;
        ServletContextHandler context = new ServletContextHandler();
        context.setDisplayName("PatchLogServer");
        MimeTypes mt = new MimeTypes();
        addMimeType(mt, Lang.TTL);
        addMimeType(mt, Lang.NT);
        addMimeType(mt, Lang.TRIG);
        addMimeType(mt, Lang.NQ);
        addMimeType(mt, Lang.RDFXML);
        context.setMimeTypes(mt);
        ErrorHandler eh = new HttpErrorHandler();
        context.setErrorHandler(eh);
        return context;
    }
    
    private static void addMimeType(MimeTypes mt, Lang lang) {
        lang.getFileExtensions().forEach(ext->
            mt.addMimeMapping(ext, lang.getContentType().getContentType())
        );
    }

    /** Build a Jetty server */
    private static Server jettyServer(int port, boolean loopback) {
        Server server = new Server();
        HttpConnectionFactory f1 = new HttpConnectionFactory();
        f1.getHttpConfiguration().setRequestHeaderSize(512 * 1024);
        f1.getHttpConfiguration().setOutputBufferSize(5 * 1024 * 1024);
        // Do not add "Server: Jetty(....) when not a development system.
        if ( true )
            f1.getHttpConfiguration().setSendServerVersion(false);
        ServerConnector connector = new ServerConnector(server, f1);
        connector.setPort(port);
        server.addConnector(connector);
        if ( loopback )
            connector.setHost("localhost");
        return server;
    }
    
    /** Build a Jetty server from a Jetty.xml configuration file */
    private static Server jettyServer(String jettyConfig) {
        try {
            LOG.info("Jetty server config file = " + jettyConfig);
            Server server = new Server();
            XmlConfiguration configuration = new XmlConfiguration(new FileInputStream(jettyConfig));
            configuration.configure(server);
            return server;
        } catch (Exception ex) {
            LOG.error("Failed to configure server: " + ex.getMessage(), ex);
            throw new DeltaConfigException("Failed to configure a server using configuration file '" + jettyConfig + "'");
        }
    }

    private void addServlet(ServletContextHandler holder, String path, Servlet servlet) {
        holder.addServlet(new ServletHolder(servlet), path);
    }
    
    private void addFilter(ServletContextHandler holder, String path, Filter filter) {
        holder.addFilter(new FilterHolder(filter), path, null);
    }

    public Integer getPort() { return port ; } 
    
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
