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
import org.seaborne.delta.Id;
import org.seaborne.delta.link.RegToken;

class DeltaAction {
    public final HttpServletRequest request;
    public final HttpServletResponse response;
    //public final Id clientId;
    public final RegToken regToken;
    
    // Should subclass but thatthen needs casting.
    
    // For RPC.
    public final String opName;
    public final JsonObject rpcArg;
    public final JsonObject requestObject;
    // For HTTP
    public final Args httpArgs;
    
    /** Basic action */
    public static DeltaAction create(HttpServletRequest request, HttpServletResponse response, Id clientId, RegToken regToken) {
        return new DeltaAction(request, response, regToken, null, null, null, null);
    }

    /** HTTP action */
    public static DeltaAction create(HttpServletRequest request, HttpServletResponse response, Id clientId, RegToken regToken, String opName, Args args) {
        return new DeltaAction(request, response, regToken, null, null, null, args);
    }
        
    /** DRPC action */
    public static DeltaAction create(HttpServletRequest request, HttpServletResponse response, RegToken regToken, String opName, JsonObject arg, JsonObject requestObject) {
        return new DeltaAction(request, response, regToken, opName, arg, requestObject, null);
    }
    
    /** DRPC action */
    private DeltaAction(HttpServletRequest request, HttpServletResponse response, 
                        RegToken regToken, 
                        String opName, JsonObject arg, JsonObject requestObject, 
                        Args args) {
        this.request = request;
        this.response = response;
        //this.clientId = clientId;
        this.regToken = regToken;
        this.opName = opName;
        this.rpcArg = arg;
        this.requestObject = requestObject;
        this.httpArgs = args;
    }
}