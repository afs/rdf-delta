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

package org.seaborne.delta.client.assembler;

import org.apache.jena.rdf.model.Property ;
import org.apache.jena.rdf.model.Resource ;
import org.apache.jena.sparql.core.assembler.AssemblerUtils ;
import org.apache.jena.sparql.core.assembler.DatasetAssemblerVocab;
import org.apache.jena.tdb.assembler.Vocab ;

public class VocabDelta {
    // Initaization - care needed:
    // Needed else:
    //    VocabDelta
    //      touches Jena
    //      causes JenaSystem.init
    //    but we are now class initializing so
    //  VocabDelta.init sees nulls in AssemblerUtils.registerDataset
    // Solution:
    //   Put AssemblerUtils.registerDataset in a static initializer separate from init()


    // Delta.namespace causes circular initialization.
    private static final String NS = "http://jena.apache.org/rdf-delta#";

    public static String getURI() { return NS ; }

    // Add feature to another (sub) dataset.
    // Thsi is ja:dataset.
    public static final Property pDeltaDataset          = Vocab.property(DatasetAssemblerVocab.getURI(), "dataset") ;

    //---- Sync'ed dataset.
    // DeltaAssembler

    // Type
    public static final Resource tDatasetDelta          = Vocab.type(getURI(), "DeltaDataset") ;

    // URL of patch log server
    public static final Property pDeltaChanges          = Vocab.property(getURI(), "changes") ;

    // Storage type("mem", tdb", "tdb2", "external").
    public static final Property pDeltaStorage          = Vocab.property(getURI(), "storage") ;

    // Whether and how often to poll for changes.
    //public static final Property pPollForChanges        = Vocab.property(NS, "poll") ;

    // Zone location for local copy.
    public static final Property pDeltaZone             = Vocab.property(getURI(), "zone") ;

    // Name of the patch log.
    public static final Property pDeltaPatchLog         = Vocab.property(getURI(), "patchlog") ;

    private static volatile boolean initialized = false ;

    static { init() ; }

    static synchronized public void init() {
        if ( initialized )
            return;
        initialized = true;
        // Not AssemblerUtils.registerDataset here -- tDatasetDelta, tLoggedDataset may be null.
        // pDeltaDataset (first) triggers jena initialization, which calls into VocalDelta.init
        // while pDeltaDataset still null.
    }

    static {
        AssemblerUtils.registerDataset(tDatasetDelta, new DeltaAssembler());
    }
}
