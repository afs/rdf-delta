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
    public static String namespace = "http://jena.apache.org/rdf-delta/v1/" ;
    private static Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    
    public final static Logger getDeltaLogger(String subName) {
        if ( subName == null || subName.isEmpty() )
            return LoggerFactory.getLogger("Delta") ;
        else
            return LoggerFactory.getLogger("Delta:"+subName) ;
    }
    
    public final static Logger DELTA_LOG        = getDeltaLogger("") ;
    public final static Logger DELTA_HTTP_LOG   = getDeltaLogger("HTTP") ;
    public final static Logger DELTA_RPC_LOG    = getDeltaLogger("RPC") ;

    static { init$() ; } 
    
    /** This is automatically called by the Jena subsystem startup cycle.
     * See {@link InitDelta} and {@code META_INF/services/org.apache.jena.system.JenaSubsystemLifecycle}
     */
    public static void init( ) {}
    
    private static void init$() {
        if ( initialized )
            return ;
        synchronized(initLock) {
            if ( initialized ) {
                JenaSystem.logLifecycle("Delta.init - return") ;
                return ;
            }
            initialized = true ;
            //DELTA_LOG.info("Initialize");
            JenaSystem.logLifecycle("Delta.init - start") ;
            // -- Nothing here at the moment -- 
            JenaSystem.logLifecycle("Delta.init - finish") ;
        }
    }
}
