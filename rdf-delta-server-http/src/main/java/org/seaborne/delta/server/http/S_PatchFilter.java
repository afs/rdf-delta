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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.link.DeltaLink;

public class S_PatchFilter implements javax.servlet.Filter {

    private final AtomicReference<DeltaLink> dLink;

    public S_PatchFilter(AtomicReference<DeltaLink> engineRef) {
        this.dLink = engineRef;
    }
    
    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        try {
            HttpServletRequest request = (HttpServletRequest)req ;
            HttpServletResponse response = (HttpServletResponse)resp ;
            
            String servletPath = request.getServletPath() ;
            String uri = request.getRequestURI() ;
            int idx1 = 0;
            if ( uri.startsWith(servletPath) )
                idx1 = servletPath.length();
            int idx2 = uri.indexOf('/', idx1);
            String x;
            if ( idx2 > idx1 )  
                // Possible name.
                x = uri.substring(idx1, idx2);
            else
                x = uri.substring(idx1);
            System.err.println("Filter match: ?? "+x);
            
            DataSourceDescription dsd = dLink.get().getDataSourceDescriptionByName(x);
            if ( dsd != null ) {
                System.err.println("Filter match:   "+x);
            }
            chain.doFilter(request, response);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {}

    @Override
    public void destroy() {}

}
