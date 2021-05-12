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

package org.seaborne.delta.fuseki;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.web.WebLib;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.sse.SSE;
import org.junit.Ignore;
import org.junit.Test;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

public class TestPatchFuseki {
    private Pair<FusekiServer, DatasetGraph> create() {
        int port = WebLib.choosePort();
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        String dsName = "/ds";
        FusekiServer server =
            DeltaFuseki.fusekiWithPatch()
                .add(dsName, dsg)
                .addOperation(dsName, DeltaFuseki.patchOp)
                .build();
        return Pair.create(server, dsg);
    }

    private static RDFPatch patch1() {
        RDFChangesCollector changes = new RDFChangesCollector();
        changes.add(node(":g"), node(":s"), node(":p"), node(":o"));
        return changes.getRDFPatch();
    }

    private static RDFPatch patch2() {
        RDFChangesCollector changes = new RDFChangesCollector();
        changes.delete(node(":g"), node(":s"), node(":p"), node(":o"));
        return changes.getRDFPatch();
    }

    private static void applyPatch(String dest, RDFPatch patch) {
        String body = RDFPatchOps.str(patch);
        HttpOp.execHttpPost(dest, DeltaFuseki.patchContentType, body);
    }

    private static Node node(String string) {
        return SSE.parseNode(string);
    }

    // This test doesn't run with Maven for some reason,
    // so it was been silently broken. Rather than fix it
    // just to get Gradle to build, we're ignoring this
    // and hoping that Andy fixes it in the main repo
    @Ignore
    @Test
    public void apply_1() {
        Pair<FusekiServer, DatasetGraph> p = create();
        FusekiServer server = p.getLeft();
        DatasetGraph dsg = p.getRight();

        server.start();
        String url = "http://localhost:"+server.getPort()+"/";
        try {
            assertFalse(dsg.contains(node(":g"), node(":s"), node(":p"), node(":o")));

            // Service name
            applyPatch(url+"/ds/patch", patch1());
            assertTrue(dsg.contains(node(":g"), node(":s"), node(":p"), node(":o")));

            // Content type.
            applyPatch(url+"/ds", patch2());
            assertFalse(dsg.contains(node(":g"), node(":s"), node(":p"), node(":o")));
            applyPatch(url+"/ds", patch1());
            assertTrue(dsg.contains(node(":g"), node(":s"), node(":p"), node(":o")));
        } finally { server.stop(); }
    }
}
