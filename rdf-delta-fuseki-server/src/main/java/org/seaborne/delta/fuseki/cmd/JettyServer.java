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

package org.seaborne.delta.fuseki.cmd;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.FusekiException;
import org.apache.jena.fuseki.jetty.FusekiErrorHandler1;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.servlets.FusekiFilter;
import org.apache.jena.fuseki.servlets.ServiceDispatchRegistry;
import org.apache.jena.riot.WebContent;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyServer {
    
    private static Logger LOG = LoggerFactory.getLogger("HTTP");

    public final Server server;
    private int port;
    
    public static Builder create() {
        return new Builder();
    }
    
    private JettyServer(int port, Server server) {
        this.server = server;
        this.port = port;
    }
    
    /** 
     * Return the port begin used.  
     * This will be the give port, which defaults to 3330, or
     * the one actually allocated if the port was 0 ("choose a free port").
     */
    public int getPort() {
        return port; 
    }

    /** Get the underlying Jetty server which has also been set up. */ 
    public Server getJettyServer() {
        return server; 
    }
    
    /** Get the {@link ServletContext}.
     * Adding new servlets is possible with care.
     */ 
    public ServletContext getServletContext() {
        return ((ServletContextHandler)server.getHandler()).getServletContext();
    }

    /** Start the server - the server continues to run after this call returns.
     *  To synchronise with the server stopping, call {@link #join}.  
     */
    public JettyServer start() { 
        try { server.start(); }
        catch (Exception e) { throw new FusekiException(e); }
        if ( port == 0 )
            port = ((ServerConnector)server.getConnectors()[0]).getLocalPort();
        LOG.info("Start (port="+port+")");
        return this;
    }

    /** Stop the server. */
    public void stop() { 
        LOG.info("Stop (port="+port+")");
        try { server.stop(); }
        catch (Exception e) { throw new FusekiException(e); }
    }
    
    /** Wait for the server to exit. This call is blocking. */
    public void join() {
        try { server.join(); }
        catch (Exception e) { throw new FusekiException(e); }
    }

    
    public static class Builder {
        private int                      port               = 3330;
        private boolean                  loopback           = false;
        private boolean                  withStats          = false;
        private boolean                  verbose            = false;
        // Other servlets to add.
        private List<Pair<String, HttpServlet>> other       = new ArrayList<>();
        
        private String                   contextPath        = "/";
        private String                   staticContentDir   = null;
        private SecurityHandler          securityHandler    = null;

        /** Set the port to run on. */ 
        public Builder setPort(int port) {
            if ( port < 0 )
                throw new IllegalArgumentException("Illegal port="+port+" : Port must be greater than or equal to zero.");
            this.port = port;
            return this;
        }
        
        /** Context path to Fuseki.  If it's "/" then Fuseki URL look like
         * "http://host:port/dataset/query" else "http://host:port/path/dataset/query" 
         */
        public Builder setContextPath(String path) {
            requireNonNull(path, "path");
            this.contextPath = path;
            return this;
        }
        
        /** Restrict the server to only responding to the localhost interface. */ 
        public Builder setLoopback(boolean loopback) {
            this.loopback = loopback;
            return this;
        }

        /** Set the location (filing system directory) to serve static file from. */ 
        public Builder setStaticFileBase(String directory) {
            requireNonNull(directory, "directory");
            this.staticContentDir = directory;
            return this;
        }
        
        /** Set a Jetty SecurityHandler.
         * <p>
         *  By default, the server runs with no security.
         *  This is more for using the basic server for testing.
         *  The full Fuseki server provides security with Apache Shiro
         *  and a defensive reverse proxy (e.g. Apache httpd) in front of the Jetty server
         *  can also be used, which provides a wide varity of proven security options.   
         */
        public Builder setSecurityHandler(SecurityHandler securityHandler) {
            requireNonNull(securityHandler, "securityHandler");
            this.securityHandler = securityHandler;
            return this;
        }
        
        /** Set verbose logging */
        public Builder setVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Add the given servlet with the pathSpec. These are added so that they are
         * checked after the Fuseki filter for datasets and before the static content
         * handler (which is the last servlet) used for {@link #setStaticFileBase(String)}.
         */
        public Builder addServlet(String pathSpec, HttpServlet servlet) {
            requireNonNull(pathSpec, "pathSpec");
            requireNonNull(servlet, "servlet");
            other.add(Pair.create(pathSpec, servlet));
            return this;
        }
        
        /**
         * Build a server according to the current description.
         */
        public JettyServer build() {
            ServletContextHandler handler = buildServletContext(contextPath);
            ServletContext cxt = handler.getServletContext();
            
            // For Fuseki servers added directly.
            Fuseki.setVerbose(cxt, verbose);
            ServiceDispatchRegistry.set(cxt, new ServiceDispatchRegistry(false));
            DataAccessPointRegistry.set(cxt, new DataAccessPointRegistry());
            
            servlets(handler);
            
            Server server = jettyServer(port, loopback);
            server.setHandler(handler);
            return new JettyServer(port, server);
        }

        /** Build a ServletContextHandler with the Fuseki router : {@link FusekiFilter} */
        private ServletContextHandler buildServletContext(String contextPath) {
            if ( contextPath == null || contextPath.isEmpty() )
                contextPath = "/";
            else if ( !contextPath.startsWith("/") )
                contextPath = "/" + contextPath;
            ServletContextHandler context = new ServletContextHandler();
            // XXX Make settable.
            context.setDisplayName("Jetty");
            context.setErrorHandler(new FusekiErrorHandler1());
            context.setContextPath(contextPath);
            if ( securityHandler != null )
                context.setSecurityHandler(securityHandler);
            
            return context;
        }
        
        private static void setMimeTypes(ServletContextHandler context) {
            MimeTypes mimeTypes = new MimeTypes();
            // RDF syntax
            mimeTypes.addMimeMapping("nt",      WebContent.contentTypeNTriples);
            mimeTypes.addMimeMapping("nq",      WebContent.contentTypeNQuads);
            mimeTypes.addMimeMapping("ttl",     WebContent.contentTypeTurtle+";charset=utf-8");
            mimeTypes.addMimeMapping("trig",    WebContent.contentTypeTriG+";charset=utf-8");
            mimeTypes.addMimeMapping("rdfxml",  WebContent.contentTypeRDFXML);
            mimeTypes.addMimeMapping("jsonld",  WebContent.contentTypeJSONLD);
            mimeTypes.addMimeMapping("rj",      WebContent.contentTypeRDFJSON);
            mimeTypes.addMimeMapping("rt",      WebContent.contentTypeRDFThrift);
            mimeTypes.addMimeMapping("trdf",    WebContent.contentTypeRDFThrift);

            // SPARQL syntax
            mimeTypes.addMimeMapping("rq",      WebContent.contentTypeSPARQLQuery);
            mimeTypes.addMimeMapping("ru",      WebContent.contentTypeSPARQLUpdate);

            // SPARQL Result set
            mimeTypes.addMimeMapping("rsj",     WebContent.contentTypeResultsJSON);
            mimeTypes.addMimeMapping("rsx",     WebContent.contentTypeResultsXML);
            mimeTypes.addMimeMapping("srt",     WebContent.contentTypeResultsThrift);

            // Other
            mimeTypes.addMimeMapping("txt",     WebContent.contentTypeTextPlain);
            mimeTypes.addMimeMapping("csv",     WebContent.contentTypeTextCSV);
            mimeTypes.addMimeMapping("tsv",     WebContent.contentTypeTextTSV);
            context.setMimeTypes(mimeTypes);
        }

        private void servlets(ServletContextHandler context) {
//            FusekiFilter ff = new FusekiFilter();
//            FilterHolder h = new FilterHolder(ff);
//            context.addFilter(h, "/*", null);

            other.forEach(p->addServlet(context, p.getLeft(), p.getRight()));
            
            if ( staticContentDir != null ) {
                DefaultServlet staticServlet = new DefaultServlet();
                ServletHolder staticContent = new ServletHolder(staticServlet);
                staticContent.setInitParameter("resourceBase", staticContentDir);
                context.addServlet(staticContent, "/");
            }
        }

        private static void addServlet(ServletContextHandler context, String pathspec, HttpServlet httpServlet) {
            ServletHolder sh = new ServletHolder(httpServlet);
            context.addServlet(sh, pathspec);
        }

        /** Jetty server */
        private static Server jettyServer(int port, boolean loopback) {
            Server server = new Server();
            HttpConnectionFactory f1 = new HttpConnectionFactory();
            // Some people do try very large operations ... really, should use POST.
            f1.getHttpConfiguration().setRequestHeaderSize(512 * 1024);
            f1.getHttpConfiguration().setOutputBufferSize(1024 * 1024);
            // Do not add "Server: Jetty(....) when not a development system.
            if ( ! Fuseki.outputJettyServerHeader )
                f1.getHttpConfiguration().setSendServerVersion(false);
            ServerConnector connector = new ServerConnector(server, f1);
            connector.setPort(port);
            server.addConnector(connector);
            if ( loopback )
                connector.setHost("localhost");
            return server;
        }


    }
}
