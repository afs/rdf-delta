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

package org.seaborne.patch.filelog;

import org.apache.jena.system.JenaSubsystemLifecycle;
import org.apache.jena.system.JenaSystem;
import org.seaborne.patch.system.InitPatch;

public class InitPatchFileLog implements JenaSubsystemLifecycle {

    public static int level = InitPatch.level+2;

    @Override
    public void start() {
        JenaSystem.logLifecycle("PatchFileLog.init - start") ;
        VocabPatch.init();
        JenaSystem.logLifecycle("PatchFileLog.init - finish") ;
    }

    @Override
    public void stop() {}

    @Override
    public int level() { return level ; }

}
