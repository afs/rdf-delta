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

package org.seaborne.delta.fuseki;

import org.apache.jena.sys.JenaSubsystemLifecycle;
import org.apache.jena.sys.JenaSystem;

public class InitDeltaFuseki implements JenaSubsystemLifecycle {

    private static volatile boolean initialized = false ;
    private static Object           initLock    = new Object() ;

    @Override
    public void start() {
        init() ;
    }

    @Override
    public void stop() { /* Do nothing */ }

    @Override
    public int level() { return 1000; }

    public static void init() {
        if ( initialized )
            return ;
        synchronized (initLock) {
            if ( initialized ) {
                JenaSystem.logLifecycle("InitDeltaFuseki.init - skip") ;
                return ;
            }
            initialized = true ;
            JenaSystem.logLifecycle("InitDeltaFuseki.init - start") ;

            JenaSystem.logLifecycle("InitDeltaFuseki.init - finish") ;

        }
    }
}
