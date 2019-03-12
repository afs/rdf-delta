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

import static org.apache.jena.sparql.sse.SSE.str;

import org.apache.jena.graph.Node;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.system.Printer;

public class RDFChangesLog implements RDFChanges {
    final private Printer printer;

    public RDFChangesLog(Printer printer) {
        this.printer = printer;
    }

    @Override
    public void start() {
        print("Start");
    }

    @Override
    public void finish() {
        print("Finish");
    }

    @Override
    public void segment() {
        print("Z");
    }

    @Override
    public void header(String field, Node value) {
        print("H   %s %s", field, str(value));
    }

    private void print(String fmt, Object... args) {
        printer.print("> "+fmt, args);
    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        print("%-3s  %s %s %s %s", "Add", strOr(g, "_"), str(s), str(p), str(o));
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        print("%-3s  %s %s %s %s", "Del", strOr(g, "_"), str(s), str(p), str(o));
    }

    public static String strOr(Node n, String alt) {
        if ( n == null )
            return alt;
        else
            return str(n);
    }

    @Override
    public void addPrefix(Node graph, String prefix, String uriStr) {
        print("AddPrefix  %s: <%s>", prefix, uriStr);
    }

    @Override
    public void deletePrefix(Node graph, String prefix) {
        print("DelPrefix  %s:", prefix);
    }

    @Override
    public void txnBegin() {
        print("Begin");
    }

    @Override
    public void txnCommit() {
        print("Commit");
    }

    @Override
    public void txnAbort() {
        print("Abort");
    }
}
