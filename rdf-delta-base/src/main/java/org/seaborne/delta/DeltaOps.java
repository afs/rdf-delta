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

package org.seaborne.delta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream ;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.jena.atlas.io.AWriter;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.seaborne.delta.lib.IOX;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.changes.RDFChangesLog ;
import org.seaborne.patch.changes.RDFChangesN ;
import org.seaborne.riot.tio.TokenWriter ;
import org.seaborne.riot.tio.impl.TokenWriterText ;

public class DeltaOps {
    
//    public static DatasetGraph managedDatasetGraph(DatasetGraph dsg, String url) {
//        RDFChangesHTTP changes = LibPatchSender.create1(url) ;
//        DatasetGraph dsg1 = new DatasetGraphChangesVersion(dsg, changes);
//        return dsg1 ;
//    }
//    
    
//    /** Called closes the {@link InputStream}. */
//    public static void play(DatasetGraph dsg, InputStream input) {
//        PatchReader pr = new PatchReader(input) ;
//        RDFChanges sc = new RDFChangesApply(dsg) ; 
//        pr.apply(sc);
//    }
    
    /** Add a printer to a {@link RDFChanges} */
    public static RDFChanges print(RDFChanges changes) {
        return RDFChangesN.multi(changes, new RDFChangesLog()) ;
    }
    
    
    /** Create a {@link TokenWriter} */
    public static TokenWriter tokenWriter(OutputStream out) {
        // Placeholder for text/binary choice.
        // IO ops to buffer
        TokenWriter tokenWriter = new TokenWriterText(out) ;
        return tokenWriter ;
    }
    
    private static NodeFormatterNT formatter = new NodeFormatterNT();

    /** Atomically write a version to disk. 
     *  <p>
     *  This is done carefully by writing to temporary file and 
     *  then doing a atomic file system move. 
     */
    public static void writeId(String filename, Id id) {
        // Get the bytes.
        Node n = id.asNode();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        AWriter aw = IO.wrapUTF8(out2);
        formatter.format(aw, n);
        byte[] bytes = out2.toByteArray();
        
        // Prepare names.
        Path p = Paths.get(filename);
        Path parent = p.getParent();
        String fn = p.getFileName().toString();
        
        if ( parent == null )
            parent = IOX.currentDirectory; 
        
        try {
            Path tmp = Files.createTempFile(parent, fn, ".tmp");
            tmp.toFile().deleteOnExit();
            IOX.safeWrite(p, tmp, (out)->{
                out.write(bytes);
            });
        } catch (IOException ex) { throw IOX.exception(ex); }
    }
}
