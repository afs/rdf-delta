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

package org.seaborne.delta.client;

import org.apache.jena.sys.JenaSubsystemLifecycle;
import org.apache.jena.sys.JenaSystem;
import org.seaborne.delta.client.assembler.VocabDelta;
import org.seaborne.delta.sys.InitDelta;

/** Delta client initialization using Jena system initialization.
 * <p>
 * <a href="https://jena.apache.org/documentation/notes/system-initialization.html">Jena system initialization</a>
 * <p>
 * See {@code DeltaSystem.init} for details of the server initialization.
 */
public class InitDeltaClient implements JenaSubsystemLifecycle {
    public static final int level = InitDelta.level+1;
    
    @Override
    public void start() {
        JenaSystem.logLifecycle("InitDeltaClient - start");
        VocabDelta.init();
        JenaSystem.logLifecycle("InitDeltaClient - finish");
    }

    @Override
    public void stop() {}

    @Override
    public int level() { return level; }
}
