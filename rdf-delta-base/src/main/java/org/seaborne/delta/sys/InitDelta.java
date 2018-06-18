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

package org.seaborne.delta.sys;

import org.apache.jena.system.JenaSubsystemLifecycle ;
import org.seaborne.delta.Delta ;
import org.seaborne.patch.system.InitPatch ;

/** General subsystem initialization using Jena system initialization.
 * <p>
 * <a href="https://jena.apache.org/documentation/notes/system-initialization.html">Jena system initialization</a>
 * <p>
 * See {@code DeltaSystem.init} for details of the server initialization.
 */
public class InitDelta implements JenaSubsystemLifecycle {
    public static final int level = InitPatch.level+10;

    @Override
    public void start() {
        Delta.init() ;
    }

    @Override
    public void stop() {}

    @Override
    public int level() { return level ; }
}
