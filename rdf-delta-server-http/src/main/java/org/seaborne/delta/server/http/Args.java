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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;

import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.RegToken;

/** Parsed arguments for Patch and Fetch. 
 * <p>
 * The query string is used for arguments.
 *  <ul>
 *  <li><tt>dataset</tt> &ndash; Id or URI for the datasource 
 *  <li><tt>patch</tt> &ndash; patch id (for fetch) 
 *  <li><tt>version</tt> &ndash; version number
 *  <li><tt>ref</tt> &ndash; pointer to predefined arguments [Not Implemented]
 *  <li><tt>zone</tt> &ndash; pointer to predefined arguments [Not Implemented] 
 *  </ul>
 */
public class Args {
    
    private static Map<String,Args> registration = new ConcurrentHashMap<>(); 

    public static Args args(HttpServletRequest request) {
        String ref = request.getParameter(DeltaConst.paramRef);
        if ( ref != null )
            return registration.get(ref);
        String zone = request.getParameter(DeltaConst.paramZone);
        String dataset = request.getParameter(DeltaConst.paramDatasource);
        String patchId = request.getParameter(DeltaConst.paramPatch);
        String version = request.getParameter(DeltaConst.paramVersion);
        
        String clientIdStr = request.getParameter(DeltaConst.paramClient);
        Id clientId = clientIdStr == null ? null : Id.fromString(clientIdStr);
        String regTokenStr = request.getParameter(DeltaConst.paramReg);  
        RegToken regToken = regTokenStr == null ? null : new RegToken(regTokenStr);
        return new Args(zone, dataset, patchId, version, clientId, regToken);
    }
    
    public final String zone;
    public final String dataset;
    public final String patchId;
    public final String version;
    public final Id clientId;
    public final RegToken regToken;

    public Args(String zone, String dataset, String patchId, String verStr, Id clientId, RegToken regToken) {
        super();
        this.zone = zone;
        this.dataset = dataset;
        this.patchId = patchId;
        this.version = verStr;
        this.clientId = clientId;
        this.regToken = regToken;
    }
}
