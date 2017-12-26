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
import org.apache.jena.tdb.assembler.Vocab ;
import org.seaborne.delta.Delta ;

public class VocabDelta {
    private static final String NS = Delta.namespace ;
    
    public static String getURI() { return NS ; } 

    // Type
    public static final Resource tDatasetDelta        = Vocab.type(NS, "DeltaDataset") ;
    
    // URL of patch log server
    public static final Property pDeltaChanges          = Vocab.property(NS, "changes") ;
    
    // Storage type("mem", tdb", "tdb2", "external").
    public static final Property pDeltaStorage            = Vocab.property(NS, "storage") ;
    
    // Whether and how often to poll for changes. 
    //public static final Property pPollForChanges     = Vocab.property(NS, "poll") ;
    
    // Zone location for local copy.
    public static final Property pDeltaZone               = Vocab.property(NS, "zone") ;
    
    // Name of the patch log. 
    public static final Property pDeltaPatchLog           = Vocab.property(NS, "patchlog") ;

    private static volatile boolean initialized = false ; 
    
    static { init() ; }
    
    static synchronized public void init() {
        if ( initialized )
            return;
        initialized = true;
        AssemblerUtils.registerDataset(tDatasetDelta, new DeltaAssembler());
    }
}
