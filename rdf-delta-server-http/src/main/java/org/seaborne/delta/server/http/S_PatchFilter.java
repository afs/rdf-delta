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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;

/** Filter that catches requests for the form /{name} and /{id}
 * where the {name} or {id} is registered with the DeltaLink and
 * directs teh request to the given servlet. 
 * Otherwise it passes the request down the filter chain.
 */ 
public class S_PatchFilter implements javax.servlet.Filter {

    private final AtomicReference<DeltaLink> dLink;
    private final HttpServlet servlet;

    public S_PatchFilter(AtomicReference<DeltaLink> engineRef, HttpServlet servlet) {
        this.dLink = engineRef;
        this.servlet = servlet;
    }
    
    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        try {
            HttpServletRequest request = (HttpServletRequest)req ;
            HttpServletResponse response = (HttpServletResponse)resp ;
            
            //request.getContextPath();
            String servletPath = request.getServletPath() ;
            String uri = request.getRequestURI() ;
            
            if ( uri.startsWith("/$") ) {
                // Direct servlets.
                chain.doFilter(request, response);
                return;
            }
            if ( uri.equals(servletPath) ) {
                // There was a match to a registered servlet.
                chain.doFilter(request, response);
                return;
            }
            
            int idx1 = 1;
            if ( servletPath.isEmpty() ) {
                // Dispatched as "/*"
                idx1 = 1 ;
            } else if ( uri.startsWith(servletPath) ) {
                if ( servletPath.endsWith("/") )
                    idx1 = servletPath.length();
                else
                    idx1 = servletPath.length()+1;
            }
            int idx2 = uri.indexOf('/', idx1);
            //System.err.printf("[%d, %d]\n", idx1, idx2);
            // Possible name or id
            String dsName;
            String trailing;
            if ( idx2 > idx1 ) {
                dsName = uri.substring(idx1, idx2);
                trailing = uri.substring(idx2+1); 
            }
            else {
                try {
                    dsName = uri.substring(idx1);
                    trailing = null;
                } catch (StringIndexOutOfBoundsException ex) {
                    System.err.println("uri = "+uri);
                    dsName = uri ;
                    throw ex;
                }
            }

            DataSourceDescription dsd = lookup(dsName);
            
            if ( dsd == null ) {
                // No match - let server routing take care of it.
                chain.doFilter(request, response);
                return ;
            }
            
//            System.err.println("uri      = "+uri);
//            System.err.println("dsName   = "+dsName);
//            System.err.println("trailing = "+trailing);
            
            // To the servlet for matches of "/{name}"
            servlet.service(request, response);
            //chain.doFilter(request, response);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // Look by name then by id
    private DataSourceDescription lookup(String x) {
        DataSourceDescription dsd = dLink.get().getDataSourceDescriptionByName(x);
        if ( dsd == null && Id.maybeUUID(x)) {
            // No name match - looks like a UUID.
            Id id = Id.parseId(x, null);
            if ( id != null )
                dsd = dLink.get().getDataSourceDescription(id);
        }
        return dsd;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {}

    @Override
    public void destroy() {}

}
