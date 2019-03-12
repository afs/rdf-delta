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

package org.seaborne.patch.changes;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.out.NodeFormatterTTL;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapFactory;
import org.seaborne.patch.RDFChanges;

/** Write data changes as SPARQL Update.
 * This is just data - no prefixes.
 */
public class RDFChangesWriteUpdate implements RDFChanges {

    private NodeFormatter formatter = new NodeFormatterNT() {
        // Write as a URI.
        @Override
        public void formatBNode(AWriter w, String label) {
            w.print("<_:");
            String lab = NodeFmtLib.encodeBNodeLabel(label);
            w.print(lab);
            w.print(">");
        }
    };

    private final AWriter out;
    // Reset every ?
    private PrefixMap pmap = PrefixMapFactory.create();

    public RDFChangesWriteUpdate(AWriter out) {
        this.out = out;
        pmap = PrefixMapFactory.create();
        this.formatter = new NodeFormatterTTL(null, pmap) {
            // Write as a URI.
            @Override
            public void formatBNode(AWriter w, String label) {
                w.print("<_:");
                String lab = NodeFmtLib.encodeBNodeLabel(label);
                w.print(lab);
                w.print(">");
            }
        };
    }

    @Override
    public void start() { }

    @Override
    public void finish() { }

    @Override
    public void header(String field, Node value) {
        header();
        out.print("# ");
        out.print(field);
        out.print(" ");
        outputNode(out, value);
        out.println();
    }

    private boolean doingHeader = true;
    private boolean adding = false;
    private boolean deleting = false;

    // Later : blocks for INSERT DATA, DELETE DATA and blocks for GRAPH

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        notHeader();
        out.print("INSERT DATA ");
        outputData(g, s, p, o);
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        notHeader();
        out.print("DELETE DATA ");
        outputData(g, s, p, o);
    }

    private void outputData(Node g, Node s, Node p, Node o) {
        out.write("{ ");
        boolean writeGraph = ( g != null );

        if ( writeGraph ) {
            out.write("GRAPH ");
            outputNode(out, g);
            out.write(" { ");
        }
        outputNode(out, s);
        out.write(" ");
        outputNode(out, p);
        out.write(" ");
        outputNode(out, o);
        out.write(" ");
        if ( writeGraph )
            out.print("} ");
        out.println(" } ;");
    }

    private void notHeader() {
        if ( doingHeader ) {
            //out.println();
            doingHeader = false;
        }
    }

    private void header() {
        if ( ! doingHeader ) {
            //out.println();
            doingHeader = true;
        }
    }

    private  void outputNode(AWriter out, Node node) {
        formatter.format(out, node);
    }

    @Override
    public void addPrefix(Node gn, String prefix, String uriStr) {
        notHeader();
        out.print("# AddPrefix ");
        if ( gn != null ) {
            outputNode(out, gn);
            out.print(" ");
        }
        out.print(prefix);
        out.print(" <");
        out.print(uriStr);
        out.print(">");
        out.println();
        out.print("PREFIX ");
        out.print(prefix+": ");
        out.print("<");
        out.print(uriStr);
        out.print(">");
        out.println();
        pmap.add(prefix, uriStr);
    }

    @Override
    public void deletePrefix(Node gn, String prefix) {
        notHeader();
        pmap.delete(prefix);
        out.print("# DelPrefix ");
        if ( gn != null ) {
            outputNode(out, gn);
            out.print(" ");
        }
        out.print(prefix);
        out.println();
    }

    @Override
    public void txnBegin() {
        notHeader();
        out.println("# Begin");
    }

    @Override
    public void txnCommit() {
        notHeader();
        out.println("# Commit");
    }

    @Override
    public void txnAbort() {
        notHeader();
        out.println("# Abort");
    }

    @Override
    public void segment() {
        notHeader();
        out.println("# Segment");
    }
}
