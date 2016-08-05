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

package embedded;

import java.util.HashMap ;
import java.util.Map ;

import org.apache.jena.fuseki.Fuseki ;
import org.apache.jena.fuseki.FusekiException ;
import org.apache.jena.fuseki.FusekiLogging ;
import org.apache.jena.fuseki.jetty.FusekiErrorHandler1 ;
import org.apache.jena.fuseki.server.* ;
import org.apache.jena.fuseki.servlets.FusekiFilter ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.eclipse.jetty.server.HttpConnectionFactory ;
import org.eclipse.jetty.server.Server ;
import org.eclipse.jetty.server.ServerConnector ;
import org.eclipse.jetty.servlet.FilterHolder ;
import org.eclipse.jetty.servlet.ServletContextHandler ;

/**
 * Embedded Fuseki server. This is a Fuseki server running with a precofigured set of
 * datasets and services. There is no admin UI.
 * <p>
 * To create a embedded sever, use {@link FusekiEmbeddedServer} ({@link #make} is a
 * packaging of a call to {@link FusekiEmbeddedServer} for the case of one dataset,
 * responding to localhost only).
 * <p>
 * A server needs to be {@link #start}ed.
 */
public class FusekiEmbeddedServer {
    // Don't create the "run/" area and don't set up logging. 
    static { 
        FusekiEnv.mode = FusekiEnv.INIT.EMBEDDED ;
        FusekiLogging.allowLoggingReset(false) ;
    }
    static void init() {}
    
    /** Construct a Fuseki server for one dataset.
     * It only responds to localhost. 
     * The returned server has not been started  */ 
    static public FusekiEmbeddedServer make(int port, String name, DatasetGraph dsg) {
        return create()
            .setPort(port)
            .setLoopback(true)
            .add(name, dsg)
            .build() ;
    }
    
    public static Builder create() {
        return new Builder() ;
    }
    
    public final Server server ;
    private int port ;
    public FusekiEmbeddedServer(Server server) {
        this.server = server ;
        port = ((ServerConnector)server.getConnectors()[0]).getPort() ;
    }
    
    /** Get the underlying Jetty server which has also been set up.
     * Adding new  */ 
    public Server getJettyServer() {
        return server ; 
    }
    
    /** Start the server - the server continues to run afetr thsi call returns.
     *  To synchronise with the server stopping, call {@link #join}.  
     */
    public void start() { 
        try { server.start(); }
        catch (Exception e) { throw new FusekiException(e) ; }
        if ( port == 0 )
            port = ((ServerConnector)server.getConnectors()[0]).getLocalPort() ;
        Fuseki.serverLog.info("Start Fuseki (port="+port+")");
    }

    /** Stop the server. */
    public void stop() { 
        Fuseki.serverLog.info("Stop Fuseki (port="+port+")");
        try { server.stop(); }
        catch (Exception e) { throw new FusekiException(e) ; }
    }
    
    /** Wait for the server to exit. This call is blocking. */
    public void join() {
        try { server.join(); }
        catch (Exception e) { throw new FusekiException(e) ; }
    }
    
    public static class Builder {
        // Simplify the DataService setup?
        // Map to list of (OperationName.Query, epName)
        
        // XXX Map <String, DataService> and do the DataAccessPoint at build time.
        
        private Map<String, DataService> map = new HashMap<>() ;
        private int port = 3333 ;
        private boolean loopback = false ;
        private String path = "/" ;
        
        /* Set the port to run on */ 
        public Builder setPort(int port) {
            this.port = port ;
            return this ;
        }
        
        /* Context path to Fuseki.  If it's "/" then Fuseki URL look like
         * "http://host:port/dataset/query" else "http://host:port/path/dataset/query" 
         */
        public Builder setContextPath(String path) {
            this.path = path ;
            return this ;
        }
        
        /** Restrict the server to only respoding to the localhost interface. */ 
        public Builder setLoopback(boolean loopback) {
            this.loopback = loopback;
            return this ;
        }

        /* Add the dataset with given name and a default set of services including update */  
        public Builder add(String name, DatasetGraph dsg) {
            return add(name, dsg, true) ;
        }

        /* Add the dataset with given name and a default set of services. */  
        public Builder add(String name, DatasetGraph dsg, boolean allowUpdate) {
            DataService dSrv = org.apache.jena.fuseki.build.Builder.buildDataService(dsg, allowUpdate) ; 
            return add(name, dSrv) ;
        }
        
        /* Add a data service that includes dataset and service names.*/  
        public Builder add(String name, DataService dataService) {
            return add$(name, dataService) ; 
        }
        
        /* Add an operation, specifing it's endpoint name.
         * This adds endpoints to any existing data service already setup by the builder.   
         */
        public Builder add(String name, DatasetGraph dsg, OperationName opName, String epName) {
            DataService dSrv = map.get(name) ;
            if ( dSrv == null ) {
                dSrv = new DataService(dsg) ;
                map.put(name, dSrv) ;
            }
            dSrv.addEndpoint(opName, epName);
            return this ; 
        }

        private Builder add$(String name, DataService dataService) {
            map.put(name, dataService) ;
            return this ;
        }
        
        /** Build a server according to the current description */ 
        public FusekiEmbeddedServer build() {
            map.forEach((name, dSrv) -> {
                DataAccessPoint dap = new DataAccessPoint(name, dSrv) ;
                DataAccessPointRegistry.get().put(name, dap) ;
            }) ;
            Server server = fusekiServer(port, path, loopback) ;
            return new FusekiEmbeddedServer(server) ;
        }

        /** build process */
        private static Server fusekiServer(int port, String contextPath, boolean loopback) {
            if ( contextPath == null || contextPath.isEmpty() )
                contextPath = "/" ;
            ServletContextHandler context = new ServletContextHandler() ;
            FusekiFilter ff = new FusekiFilter() ;
            FilterHolder h = new FilterHolder(ff) ;
            context.setContextPath(contextPath); 
            context.addFilter(h, "/*", null);
            context.setDisplayName(Fuseki.servletRequestLogName);  
            context.setErrorHandler(new FusekiErrorHandler1());
            Server server = defaultServerConfig(port, loopback) ;
            server.setHandler(context);
            return server ;
        }

        /** Jetty build process */
        private static Server defaultServerConfig(int port, boolean loopback) {
            Server server = new Server() ;
            HttpConnectionFactory f1 = new HttpConnectionFactory() ;
            // Some people do try very large operations ... really, should use POST.
            f1.getHttpConfiguration().setRequestHeaderSize(512 * 1024);
            f1.getHttpConfiguration().setOutputBufferSize(5 * 1024 * 1024) ;
            // Do not add "Server: Jetty(....) when not a development system.
            if ( ! Fuseki.outputJettyServerHeader )
                f1.getHttpConfiguration().setSendServerVersion(false) ;
            ServerConnector connector = new ServerConnector(server, f1) ;
            connector.setPort(port) ;
            server.addConnector(connector);
            if ( loopback )
                connector.setHost("localhost");
            return server ;
        }
    }
}