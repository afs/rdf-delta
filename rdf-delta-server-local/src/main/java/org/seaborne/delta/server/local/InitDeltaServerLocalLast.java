/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.local;

import org.seaborne.delta.server.system.DeltaSubsystemLifecycle ;
import org.seaborne.delta.server.system.DeltaSystem ;

public class InitDeltaServerLocalLast implements DeltaSubsystemLifecycle {

    @Override
    public void start() {
        DeltaSystem.logLifecycle("InitDeltaServerLocalLast - start");
        DPS.initLast();
        DeltaSystem.logLifecycle("InitDeltaServerLocalLast - finish");
    }

    @Override
    public void stop() {}

    // Make this late so DPS can do setup all normal registrations.
    @Override
    public int level() { return 9000 ; }
}
