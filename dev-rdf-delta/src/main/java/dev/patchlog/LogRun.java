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

package dev.patchlog;

import java.net.BindException;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.logging.LogCtl;
import org.seaborne.delta.Delta;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.delta.server.http.PatchLogServer;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;

public class LogRun {

    static { LogCtl.setJavaLogging(); }
    
    public static void main(String[] args) {
        try {
            main$();
        } finally { System.exit(0);}
    }
    
    public static void main$() {
        FileOps.ensureDir("Zone");
        FileOps.clearAll("Zone");
        Delta.init();
        
        int D_PORT = 1068;
        
        PatchLogServer dps = server(D_PORT, "DeltaServer");
        String URL = "http://localhost:"+D_PORT+"/";       
        
        DeltaLink dLink = DeltaLinkHTTP.connect("http://localhost:"+D_PORT+"/"); 
        Id clientId = Id.create();
        RegToken regToken = dLink.register(clientId);
        Id dsRef = dLink.newDataSource("ABC", "http://example/ABC");
        
        PatchLogHTTP log = new PatchLogHTTP("http://localhost:"+D_PORT+"/", regToken);
        RDFPatch patch = RDFPatchOps.read("/home/afs/ASF/rdf-delta/rdf-delta-test/testing/test_dlink/patch1.rdfp");
        
        System.out.println("\nAppend\n");
        
        long version = log.append("ABC", patch);
        
        // ******
        
        System.out.printf("version = %d\n", version);
        System.out.println("\nFetch\n");
        RDFPatch patch2 = log.fetch("ABC", version);
        RDFPatchOps.write(System.out, patch2);
    }

    private static PatchLogServer server(int port, String base) {
        // --- Reset state.
        FileOps.ensureDir(base);
        FileOps.clearAll("DeltaServer/ABC");
        FileOps.delete("DeltaServer/ABC");
        FileOps.clearAll(base);
        PatchLogServer dps = PatchLogServer.server(port, base);
        try { 
            dps.start();
            return dps; 
        } catch(BindException ex) {
            Delta.DELTA_LOG.error("Address in use: port="+port);
            System.exit(0);
            return null;
        }
    }
}
