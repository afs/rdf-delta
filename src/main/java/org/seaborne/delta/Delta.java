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
import org.seaborne.delta.assembler.VocabDelta ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class Delta {
    public static String namespace = "http://jena.apache.org/rdf-delta/v1/" ;
    private static Object initLock = new Object() ;
    private static volatile boolean initialized = false ;
    
    public static Logger deltaLog = LoggerFactory.getLogger("Delta") ; 

    static { init$() ; } 
    
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
            deltaLog.info("Initialize");
            JenaSystem.logLifecycle("Delta.init - start") ;
            VocabDelta.init();    
            JenaSystem.logLifecycle("Delta.init - finish") ;
        }
    }
}
