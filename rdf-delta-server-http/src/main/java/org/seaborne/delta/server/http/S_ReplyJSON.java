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

import org.apache.jena.atlas.io.IndentedWriter ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.riot.WebContent ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.slf4j.Logger ;

/** Base class for responds with JSON object */
public abstract class S_ReplyJSON extends HttpServlet {
    static private Logger LOG = Delta.DELTA_LOG ;

    public S_ReplyJSON() { }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        json(req, resp, json(req));
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        json(req, resp, json(req));
    }

    protected abstract JsonValue json(HttpServletRequest req) ;

    public static void json(HttpServletRequest req, HttpServletResponse resp, JsonValue responseContent) {
        try {
            resp.setHeader(HttpNames.hCacheControl, "no-cache");
            resp.setHeader(HttpNames.hContentType,  WebContent.contentTypeJSON);
            resp.setStatus(HttpSC.OK_200);
            try(ServletOutputStream out = resp.getOutputStream(); IndentedWriter b = new IndentedWriter(out); ) {
                b.setFlatMode(true);
                JSON.write(b, responseContent);
                b.ensureStartOfLine();
                b.flush();
                out.write('\n');
            }
        } catch (IOException ex) {
            LOG.warn("json: IOException", ex);
            try {
                resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Internal server error");
            } catch (IOException ex2) {}
        }
    }
}
