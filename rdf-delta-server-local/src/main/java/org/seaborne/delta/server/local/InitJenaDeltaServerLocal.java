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

package org.seaborne.delta.server.local;

import org.apache.jena.sys.JenaSubsystemLifecycle ;
import org.apache.jena.sys.JenaSystem;
import org.seaborne.delta.server.system.DeltaSystem ;

/** Hook into JenaSystem initialization.
 * @see InitDeltaServerLocal
 */
public class InitJenaDeltaServerLocal implements JenaSubsystemLifecycle {
    // Not used - kept for the information.
    // InitDelatServerLocal is called from the Delta server initialization sequence.
    @Override
    public void start() {
        boolean original = DeltaSystem.DEBUG_INIT;
        DeltaSystem.DEBUG_INIT = DeltaSystem.DEBUG_INIT | JenaSystem.DEBUG_INIT;
        JenaSystem.logLifecycle("InitJenaDeltaServerLocal - start");
        // Delta's (newer) version of the same initialization system using different names. 
        //DeltaSystem.init();
        JenaSystem.logLifecycle("InitJenaDeltaServerLocal - finish");
        DeltaSystem.DEBUG_INIT = original;
    }
    
    @Override
    public void stop() {}

    @Override
    public int level() { return 100; }
}
