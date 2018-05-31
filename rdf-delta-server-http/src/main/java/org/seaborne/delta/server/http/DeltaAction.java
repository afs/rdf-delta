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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.json.JsonObject;
import org.seaborne.delta.DeltaBadRequestException ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;

public class DeltaAction {
    public final HttpServletRequest request;
    public final HttpServletResponse response;
    public final DeltaLink dLink;
    public final RegToken regToken;
    public final String opName;
    
    // Should subclass but that then needs casting.
    
    // For RPC.
    public final JsonObject rpcArg;
    public final JsonObject requestObject;
    // For HTTP
    public final Args httpArgs;
    
//    /** Basic action */
//    public static DeltaAction create(HttpServletRequest request, HttpServletResponse response, Id clientId, RegToken regToken) {
//        return new DeltaAction(request, response, regToken, null, null, null, null);
//    }

    /** HTTP action */
    public static DeltaAction create(HttpServletRequest request, HttpServletResponse response, 
                                     DeltaLink deltaLink, RegToken regToken,
                                     String opName, String opId, Args args) {
        return new DeltaAction(request, response, deltaLink, regToken, opName, opId, null, null, args);
    }
        
    /** DRPC action */
    public static DeltaAction create(HttpServletRequest request, HttpServletResponse response, 
                                     DeltaLink deltaLink, RegToken regToken,
                                     String opName, String opId, JsonObject arg, JsonObject requestObject) {
        return new DeltaAction(request, response, deltaLink, regToken, opName, opId, arg, requestObject, null);
    }
    
    /** DRPC action */
    private DeltaAction(HttpServletRequest request, HttpServletResponse response, 
                        DeltaLink deltaLink, RegToken regToken, 
                        String opName, String opId, JsonObject arg, JsonObject requestObject, 
                        Args args) {
        this.request = request;
        this.response = response;
        this.dLink = deltaLink;
        this.regToken = regToken;
        this.opName = opName;
        this.rpcArg = arg;
        this.requestObject = requestObject;
        this.httpArgs = args;
    }

    /** Return the full URL, including the query string */
    public String getURL() {
        StringBuffer sBuff = request.getRequestURL();
        String queryString = request.getQueryString();
        if ( queryString != null )
            sBuff.append('?').append(queryString);
        return sBuff.toString(); 
    }
    
    public static void errorBadRequest(String msg) {
        throw new DeltaBadRequestException(msg);
    }
}