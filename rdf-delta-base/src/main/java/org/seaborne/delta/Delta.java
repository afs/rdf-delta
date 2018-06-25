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

package org.seaborne.delta;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.sys.JenaSystem ;
import org.seaborne.delta.sys.InitDelta ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class Delta {
    static { JenaSystem.init(); } 
    
    public static String namespace = "http://jena.apache.org/rdf-delta/" ;
    private static volatile boolean initialized = false ;
    // Operations log - not development debugging.
    private static String LoggerNameBase = "Delta";
    
    public final static Logger getDeltaLogger(String subName) {
        if ( subName == null || subName.isEmpty() )
            return LoggerFactory.getLogger(LoggerNameBase) ;
        else
            return LoggerFactory.getLogger(LoggerNameBase+"."+subName) ;
    }
    
    // For dynamically change logging (during development)
    
    /** Switch logging on from this point */
    public final static void enableDeltaLogging() {
        LogCtl.setInfo(LoggerNameBase); 
    }
    
    /** Switch logging off */
    public final static void disableDeltaLogging() {
        LogCtl.disable(LoggerNameBase);
    }
    
    public final static Logger DELTA_LOG        = getDeltaLogger("Delta") ;
    // Client operations
    public final static Logger DELTA_CLIENT     = getDeltaLogger("Delta") ;
    // Unused?
    public final static Logger DELTA_PATCH      = getDeltaLogger("Patch") ;
    // HTTP actions.
    public final static Logger DELTA_HTTP_LOG   = getDeltaLogger("HTTP") ;
    // RPC actions
    public final static Logger DELTA_RPC_LOG    = getDeltaLogger("RPC") ;
    // Configuration.
    public final static Logger DELTA_CONFIG_LOG = getDeltaLogger("Config") ;

    /** This is automatically called by the Jena subsystem startup cycle.
     * See {@link InitDelta} and {@code META_INF/services/org.apache.jena.system.JenaSubsystemLifecycle}
     * (not the {@code DeltaSystem} initialization) 
     */
    public static void init( ) { }
    
}
