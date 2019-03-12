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

package org.seaborne.delta.server.http;

import java.io.IOException ;
import java.nio.charset.StandardCharsets ;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.riot.WebContent ;
import org.eclipse.jetty.server.Request ;
import org.eclipse.jetty.server.Response ;
import org.eclipse.jetty.server.handler.ErrorHandler ;

public class HttpErrorHandler extends /*Jetty*/ErrorHandler {
    public static final String METHOD_DELETE    = "DELETE" ;
    public static final String METHOD_HEAD      = "HEAD" ;
    public static final String METHOD_GET       = "GET" ;
    public static final String METHOD_OPTIONS   = "OPTIONS" ;
    public static final String METHOD_POST      = "POST" ;
    public static final String METHOD_PUT       = "PUT" ;
    public static final String METHOD_TRACE     = "TRACE" ;
    public static final String METHOD_PATCH     = "PATCH" ;
    
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if ( request.getMethod().equals(METHOD_POST)) {
            response.setContentType(WebContent.contentTypeTextPlain);
            response.setCharacterEncoding(WebContent.charsetUTF8) ;
            
            String reason=(response instanceof Response)?((Response)response).getReason():null;
            String msg = String.format("%03d %s\n", response.getStatus(), reason) ;
            response.getOutputStream().write(msg.getBytes(StandardCharsets.UTF_8)) ;
            
            response.getOutputStream().flush() ;
            baseRequest.setHandled(true);
            return;
        }
        super.handle(target, baseRequest, request, response); 
    }
    
//    // Single line for HTTP POST
//    @Override
//    protected void handleErrorPage(HttpServletRequest request, Writer writer, int code, String message) throws IOException {
//        if ( request.getMethod().equals(METHOD_POST)) {
//        } else {
//            super.handleErrorPage(request, writer, code, message);
//        }
//    }
}
