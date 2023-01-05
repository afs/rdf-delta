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

package org.seaborne.delta.cmds;

import java.io.InputStream ;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.rdfpatch.PatchHeader;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.RDFPatchOps;
import org.apache.jena.rdfpatch.changes.RDFChangesBase;

/** Converted  patch into reified triples (extended reification - adds "rdf:graph" to get quads) */
/*public*/ class patchreif extends CmdPatch
{
    static {
        LogCtl.setLogging();
        JenaSystem.init();
    }

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
