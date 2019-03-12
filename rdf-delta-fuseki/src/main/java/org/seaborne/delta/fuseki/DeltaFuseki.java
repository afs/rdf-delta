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

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.Operation ;
import org.apache.jena.fuseki.servlets.ActionService ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatchOps ;

/** Library of operations. */
public class DeltaFuseki {
    public static FusekiServer fuseki(int port, String config) {
        return FusekiServer.create().port(port).parseConfigFile(config).build().start();
    }

    /**
     * Build a Fuseki server whose dataset is a changes dataset wrapping the given base.
     */
    public static FusekiServer deltaFuseki(int port, String name, DatasetGraph dsgBase, RDFChanges changes) {
        DatasetGraph dsg = RDFPatchOps.changes(dsgBase, changes);
        return
            FusekiServer.create().port(port)
                .add(name, dsg)
                .enablePing(true)
                .build();
    }

    public static Operation patchOp = Operation.register("Patch", "Patch Service");
    public static String patchContentType = "application/rdf-patch";

    /** Create a {@code FusekiServer.Builder} with registered patch operation.
     *  To enable it, on a dataset "name", use {@code addOperation(name, "patch", DeltaFuseki.patchOp)}.
     *  This makes it available by POST to {@code /name/patch} or {@code /name} with content-type "application/rdf-patch".
     */
    public static FusekiServer.Builder fusekiWithPatch() {
        ActionService handler = new PatchApplyService();
        return FusekiServer.create().registerOperation(patchOp, patchContentType, handler);
    }
}
