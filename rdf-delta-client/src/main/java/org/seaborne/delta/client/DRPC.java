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

package org.seaborne.delta.client;

import java.io.IOException ;
import java.util.Objects ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.json.JSON ;
import org.apache.jena.atlas.json.JsonException ;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.web.TypedInputStream ;
import org.apache.jena.riot.WebContent ;
import org.apache.jena.riot.web.HttpOp ;
import org.seaborne.delta.DPNames ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.lib.J ;
import org.slf4j.Logger ;

public class DRPC {
    static private Logger LOG = Delta.DELTA_RPC_LOG ; 
    
    /** Send a JSON argument to a URL+name by POST and received a JSON object in return. */
    public static JsonValue rpc(String url, String opName, JsonValue arg) {
        JsonObject a = J.buildObject((b)->
            b.key(DPNames.F_OP).value(opName)
             .key(DPNames.F_ARG).value(arg)
            ) ;
        return rpc(url, a) ;
    }
    
    /** Send a JSON object to a URL by POST and received a JSON object in return. */
    public static JsonValue rpc(String url, JsonObject object) {
        Objects.requireNonNull(url, "DRPC.rpc: Arg1 URL is null") ;
        Objects.requireNonNull(object, "DRPC.rpc: Arg2 JSON object is null") ;

        if ( ! object.hasKey(DPNames.F_OP) )
            throw new DeltaException() ; 
        
        String argStr = JSON.toString(object) ;
        try (TypedInputStream x = HttpOp.execHttpPostStream(url, 
                                                            WebContent.contentTypeJSON, argStr,
                                                            WebContent.contentTypeJSON)) {
            if ( x == null )
                throw new JsonException("No response") ;
            
            if ( true ) {
                try {
                    String s = IO.readWholeFileAsUTF8(x) ;
                    return JSON.parseAny(s) ;
                } catch (IOException ex) { IO.exception(ex); return null ;}
            }
            else 
                return JSON.parseAny(x) ;
        }
    }
    
    //public static JsonValue rpc(String url, JsonObject object) {
}
