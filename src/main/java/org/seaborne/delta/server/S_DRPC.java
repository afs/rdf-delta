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

package org.seaborne.delta.server;

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.io.PrintStream ;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.io.IndentedLineBuffer ;
import org.apache.jena.atlas.json.* ;
import org.apache.jena.web.HttpSC ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

/** Receive a JSON object, return a JSON object */ 
public class S_DRPC extends ServletBase {
    static public Logger         LOG     = LoggerFactory.getLogger("RPC") ;
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        JsonValue arg ;
        try (InputStream in = req.getInputStream()  ) {
            arg = JSON.parseAny(in) ;
        } catch (JsonException ex) {
            LOG.warn("400 "+ex.getMessage()) ;
            resp.sendError(HttpSC.BAD_REQUEST_400, "JSON exception: "+ex.getMessage()) ;
//            OutputStream out = resp.getOutputStream() ;
//            PrintStream ps = new PrintStream(out) ; 
//            ex.printStackTrace(ps);
//            ps.flush();
            return ;
        }
        
        LOG.info("Arg: "+arg) ;
        
        OutputStream out = resp.getOutputStream() ;
        try {
            JsonValue rslt = call(arg) ;
            try ( IndentedLineBuffer x = new IndentedLineBuffer() ) {
                x.setFlatMode(true);
                JSON.write(x, rslt); 
                LOG.info("Result: "+x.asString()) ;
            }
            resp.setStatus(HttpSC.OK_200);
            JSON.write(out, rslt);
        }
        catch (Throwable ex) {
            LOG.warn("500 "+ex.getMessage(), ex) ;
            resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Exception: "+ex.getMessage()) ;
            PrintStream ps = new PrintStream(out) ; 
            ex.printStackTrace(ps);
            ps.flush();
            return ;
        }   
    }
    
    protected JsonValue call(JsonValue arg) {
        if ( arg.isObject() ) {
            JsonObject x = copy(arg).getAsObject() ;
            x.put("result", x.get("integer").getAsNumber().value().longValue()+1) ;
            return x ;
        }
        if ( arg.isArray() ) {
            JsonArray x = copy(arg).getAsArray() ;
            x.add(999) ;
            return x ;
        }
        return arg ;
    }
    
    // ==> JSON.
    
    /** Create a safe copy of a {@link JsonValue}.
     * <p>
     *  If the JsonValue is a structure (object or array), copy the structure recursively.
     *  <p>
     *  If the JsonValue is a primitive (string, number, boolean or null),
     *  it is immutable so return the same object.  
     */
    public static JsonValue copy(JsonValue arg) {
        JsonBuilder builder = builder(arg) ;
        return builder==null ? arg : builder.build() ;
    }
    
    /** Create a builder from a {@link JsonValue}.
     *  <p>If the argument is an object or array, use it to initailize the builder.
     *  <p>if the argument is a JSON primitive (string, number, boolean or null),
     *  return null (the {@code JsonValue} is immutable).
     */
    public static JsonBuilder builder(JsonValue arg) {
        if ( arg.isObject() ) {
            JsonObject obj = arg.getAsObject() ;
            JsonBuilder builder = JsonBuilder.create() ;
            builder.startObject() ;
            obj.forEach((k,v) -> builder.key(k).value(copy(v))) ;
            builder.finishObject() ;
            return builder ; 
        }
        if ( arg.isArray() ) {
            JsonArray array = arg.getAsArray() ;
            JsonBuilder builder = JsonBuilder.create() ;
            builder.startArray() ;
            array.forEach((a)->builder.value(copy(a))) ;
            builder.finishArray() ;
            return builder ; 
        }
        return null ;
    }
}
