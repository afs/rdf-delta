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

import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.server.local.Patch;
import org.seaborne.delta.server.local.PatchHandler;
import org.apache.jena.rdfpatch.RDFChanges ;
import org.apache.jena.rdfpatch.changes.RDFChangesApply ;

/** Write a patch to a {@link DatasetGraph}. */ 
public class PHandlerLocalDB implements PatchHandler {

    private final DatasetGraph dsg ;

    public PHandlerLocalDB(DatasetGraph dsg) {
        this.dsg = dsg ;
    }
    
    @Override
    public void handle(Patch patch) {
        RDFChanges changes = new RDFChangesApply(dsg) ;
        patch.play(changes); 
    }
}
