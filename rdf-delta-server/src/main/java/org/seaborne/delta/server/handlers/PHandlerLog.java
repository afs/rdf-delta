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

package org.seaborne.delta.server.handlers;

import org.apache.jena.atlas.logging.FmtLog ;
import org.seaborne.delta.server.PatchHandler ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.changes.RDFChangesCounter ;
import org.seaborne.patch.changes.RDFChangesOnStartFinish ;
import org.slf4j.Logger ;

public class PHandlerLog implements PatchHandler {
    
    private final Logger log ;
    
    public PHandlerLog(Logger log) {
        this.log = log ;
    }
    
    /** Safe handler */
    @Override
    public RDFChanges handler() {
        RDFChangesCounter scc = new RDFChangesCounter() ;
        return new RDFChangesOnStartFinish(scc,
                                           null,
                                           ()-> FmtLog.info(log,
                                                            "Patch: Quads: add=%d, delete=%d :: Prefixes: add=%d delete=%d",
                                                            scc.countAddQuad, scc.countDeleteQuad, 
                                                            scc.countAddPrefix, scc.countDeletePrefix
                                               ));
    }
}
