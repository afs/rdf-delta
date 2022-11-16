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

package org.seaborne.delta.server.local.handlers;

import org.apache.jena.atlas.logging.FmtLog ;
import org.seaborne.delta.server.local.Patch;
import org.seaborne.delta.server.local.PatchHandler;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.rdfpatch.changes.PatchSummary;
import org.slf4j.Logger ;

/** Log a infroamtion about a patch */
public class PHandlerLog implements PatchHandler {
    
    private final Logger log ;
    
    public PHandlerLog(Logger log) {
        this.log = log ;
    }
    
    /** Safe handler */
    @Override
    public void handle(Patch patch) {
        
        PatchSummary scc = RDFPatchOps.summary(patch) ;
        FmtLog.info(log,
                    "Patch: Quads: add=%d, delete=%d :: Prefixes: add=%d delete=%d",
                    scc.countAddData, scc.countDeleteData, 
                    scc.countAddPrefix, scc.countDeletePrefix) ;
    }
}
