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

package org.seaborne.patch;

import java.io.OutputStream ;
import java.util.UUID ;

import org.apache.jena.graph.Node ;
import org.apache.jena.graph.NodeFactory ;
import org.apache.jena.graph.Triple ;
import org.apache.jena.riot.system.StreamRDF ;
import org.apache.jena.sparql.core.Quad ;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.patch.text.TokenWriter ;
import org.seaborne.patch.text.TokenWriterText ;

/** Convert a {@link StreamRDF} to a {@code RDF Patch}.
 *  {@link StreamRDF#start} and {@link StreamRDF#finish}
 *  must be called; these bracket the patch in transaction markers 
 *  {@code TX} and {@code TC}.  
 */
public class StreamPatch implements StreamRDF {

    private int depth = 0;
    private OutputStream out ;
    private RDFChanges c;
    
    public StreamPatch(OutputStream out) {
        this.out = out;
        TokenWriter t = new TokenWriterText(out);
        this.c = new RDFChangesWriter(t);
    }
    
    @Override
    public void start() {
        depth++ ;
        if ( depth == 1 ) {
            // Header
            // Node n = NodeFactory.createURI(JenaUUID.getFactory().generate().asURI());
            Node n = NodeFactory.createURI("uuid:"+UUID.randomUUID().toString());
            c.header(RDFPatchConst.ID, n);
            c.txnBegin();
        }
    }
    

    @Override
    public void triple(Triple triple) {
        c.add(null, triple.getSubject(), triple.getPredicate(), triple.getObject());
    }

    @Override
    public void quad(Quad quad) {
        c.add(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    @Override
    public void base(String base) {}

    @Override
    public void prefix(String prefix, String iri) {
        c.addPrefix(null, prefix, iri);
    }

    @Override
    public void finish() {
        if ( depth == 1 )
            c.txnCommit();
        --depth ;
    }
}