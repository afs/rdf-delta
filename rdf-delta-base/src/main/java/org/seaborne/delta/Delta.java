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

import org.apache.jena.system.JenaSystem ;
import org.seaborne.delta.sys.InitDelta ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class Delta {
    public static String namespace = "http://jena.apache.org/rdf-delta/" ;
    private static Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    private static String LoggerNameBase = "Delta";
    
    public final static Logger getDeltaLogger(String subName) {
        if ( subName == null || subName.isEmpty() )
            return LoggerFactory.getLogger(LoggerNameBase) ;
        else
            return LoggerFactory.getLogger(LoggerNameBase+"."+subName) ;
    }
    
    public final static Logger DELTA_LOG        = getDeltaLogger("") ;
    public final static Logger DELTA_HTTP_LOG   = getDeltaLogger("HTTP") ;
    public final static Logger DELTA_RPC_LOG    = getDeltaLogger("RPC") ;
    public final static Logger DELTA_CONFIG_LOG = getDeltaLogger("Config") ;

    static { JenaSystem.init(); } 
    
    /** This is automatically called by the Jena subsystem startup cycle.
     * See {@link InitDelta} and {@code META_INF/services/org.apache.jena.system.JenaSubsystemLifecycle}
     */
    public static void init( ) { init$(); }
    
    private static void init$() {
        if ( initialized )
            return ;
        synchronized(initLock) {
            initialized = true ;
            //DELTA_LOG.info("Initialize");
            // -- Nothing here at the moment -- 
        }
    }
}
