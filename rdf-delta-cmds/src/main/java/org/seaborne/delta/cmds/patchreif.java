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

package org.seaborne.delta.cmds;

import java.io.InputStream ;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.system.JenaSystem;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesBase;

/** Converted  patch into reified triples (extended reification - adds "rdf:graph" to get quads) */
/*public*/ class patchreif extends CmdPatch
{
    static { JenaSystem.init(); LogCtl.setCmdLogging() ; }
    
    public static void main(String... args) {
        new patchreif(args).mainRun();
    }

    public patchreif(String[] argv) {
        super(argv) ;
    }
    
    @Override
    protected String getCommandName() {
        return "patchparse";
    }

    // Whether to directly write or accumulate in a graph.
    private Graph graph = GraphFactory.createDefaultGraph(); 
    
    @Override
    protected void execStart() { }
    
    @Override
    protected void execFinish() {}
    
    @Override
    protected void execOne(String source, InputStream input) {
        RDFPatch patch = RDFPatchOps.read(input);
        
        PatchHeader header = patch.header();
        
        RDFChanges c = new RDFChangesBase() {
            @Override
            public void add(Node g, Node s, Node p, Node o) {}

            @Override
            public void delete(Node g, Node s, Node p, Node o) { }
        };
        
        patch.apply(c);
    }
}
