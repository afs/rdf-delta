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

import org.seaborne.delta.DPNames;

/** Parsed argument */
public class Args {
    
    private static Map<String,Args> registration = new ConcurrentHashMap<>(); 

    public static Args args(HttpServletRequest request) {
        String ref = request.getParameter(DPNames.paramRef);
        if ( ref != null )
            return registration.get(ref);
        String zone = request.getParameter(DPNames.paramZone);
        String dataset = request.getParameter(DPNames.paramDataset);
        String patchId = request.getParameter(DPNames.paramPatch);
        String version = request.getParameter(DPNames.paramVersion);
        return new Args(zone, dataset, patchId, version);
    }
    
    public final String zone;
    public final String dataset;
    public final String patchId;
    public final String version;

    public Args(String zone, String dataset, String patchId, String verStr) {
        super();
        this.zone = zone;
        this.dataset = dataset;
        this.patchId = patchId;
        this.version = verStr;
    }
}
