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

import javax.servlet.ServletOutputStream ;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.riot.WebContent ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.apache.jena.atlas.io.IOX.IOConsumer;
import org.slf4j.Logger ;

/** Utility servlet that responds 200 and a plain text message */
public class S_ReplyText extends HttpServlet {
    static private Logger LOG = Delta.DELTA_LOG ;
    private final IOConsumer<ServletOutputStream> output;
    
    // Source of content.
    public S_ReplyText(IOConsumer<ServletOutputStream> output) { this.output = output; }
    
    public S_ReplyText(String message) { 
        this.output = (x)->x.print(message);
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        text(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        text(req, resp);
    }
    
    private void text(HttpServletRequest req, HttpServletResponse resp) {
        try { 
            resp.setHeader(HttpNames.hContentType,  WebContent.contentTypeTextPlain);
            resp.setStatus(HttpSC.OK_200);
            try(ServletOutputStream out = resp.getOutputStream(); ) {
                output.actionEx(out);
            }
        } catch (IOException ex) {
            LOG.warn("text out: IOException", ex);
            try { 
                resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Internal server error");
            } catch (IOException ex2) {}
        }
    }
}
