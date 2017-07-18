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

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.util.Collections;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.shared.uuid.JenaUUID;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.patch.changes.*;
import org.seaborne.patch.system.DatasetGraphChanges ;
import org.seaborne.riot.tio.TokenWriter ;
import org.seaborne.riot.tio.impl.TokenWriterText ;

public class RDFPatchOps {
    public static String namespace = "http://jena.apache.org/rdf-patch/" ;
    
    /** Read an {@link RDFPatch} from a file. */
    public static RDFPatch fileToPatch(String filename) {
        InputStream in = IO.openFile(filename) ;
        return read(in) ;
    }

    private static class RDFPatchNull implements RDFPatch {
        private final PatchHeader header = new PatchHeader(Collections.emptyMap());
        @Override
        public PatchHeader header() {
            return header();
        }
        @Override
        public void apply(RDFChanges changes) {}
    }

    private static class RDFPatchEmpty implements RDFPatch {
        private final Node id = NodeFactory.createURI(JenaUUID.generate().asURI());
        private final PatchHeader header = new PatchHeader(Collections.singletonMap(RDFPatch.ID, id));
        
        RDFPatchEmpty() {}
        
        @Override
        public PatchHeader header() {
            return header;
        }
        @Override
        public void apply(RDFChanges changes) {
            header.apply(changes);
            changes.txnBegin();
            changes.txnCommit();
        }
    }
    
    /** A immutable "nullop" patch - no transaction, no id. */
    public static RDFPatch nullPatch() {
        return new RDFPatchNull();
    }
    
    /** An immutable "empty" patch - a single transaction of no changes.
     * Each call generates a new empty patch with a different id.
     */
    public static RDFPatch emptyPatch() {
        return new RDFPatchEmpty();
    }

    /** Create a brief summary of a patch.
     * <p> 
     * This function plays the patch.
     * */
    public static PatchSummary summary(RDFPatch patch) {
        RDFChangesCounter x = new RDFChangesCounter();
        patch.apply(x);
        return x.summary();
    }
    
    /** Make sure a patch has been read from its input.
     *  The returned {@link RDFPatch} is not connected
     *  to an external resource like an {@link InputStream}.  
     */
    public static RDFPatch collect(RDFPatch patch) {
        if ( patch instanceof RDFChangesCollector )
            return patch;
        RDFChangesCollector x = new RDFChangesCollector();
        patch.apply(x);
        return x.getRDFPatch();
    }
    

    
    /** Read an {@link RDFPatch} from a file. */
    public static RDFPatch read(InputStream input) {
        RDFPatchReaderText pr = new RDFPatchReaderText(input) ;
        RDFChangesCollector c = new RDFChangesCollector() ;
        pr.apply(c);
        return c.getRDFPatch() ; 
    }

    /** Read an {@link RDFPatch} from a file. */
    public static RDFPatch read(String filename) {
        try ( InputStream input = IO.openFile(filename) ) {
            return read(input);
        } catch (IOException ex) { IO.exception(ex); return null; }
    }

    /** Read an {@link RDFPatch} header. */
    public static PatchHeader readHeader(InputStream input) {
        return RDFPatchReaderText.readerHeader(input); 
    }

    /** Apply changes from a {@link RDFPatch} to a {@link DatasetGraph} */ 
    public static void applyChange(DatasetGraph dsg, RDFPatch patch) {
        RDFChanges changes = new RDFChangesApply(dsg) ;
        patch.apply(changes);
    }
    
    /** Apply changes from a text format input stream to a {@link DatasetGraph} */ 
    public static void applyChange(DatasetGraph dsg, InputStream input) {
        RDFPatchReaderText pr = new RDFPatchReaderText(input) ;
        RDFChanges changes = new RDFChangesApply(dsg) ;
        pr.apply(changes);
    }

    /** Create a {@link DatasetGraph} that sends changes to a {@link RDFChanges} stream */ 
    public static DatasetGraph changes(DatasetGraph dsgBase, RDFChanges changes) {
        return new DatasetGraphChanges(dsgBase, changes) ;
    }
    
    /** Create a {@link DatasetGraph} that writes changes to an{@link OutputStream} in text format.
     *  The caller is responsible for closing the {@link OutputStream}.
     */ 
    public static DatasetGraph changesAsText(DatasetGraph dsgBase, OutputStream out) {
        TokenWriter tokenWriter = new TokenWriterText(out) ;
        RDFChanges changeLog = new RDFChangesWriter(tokenWriter) ;
        return changes(dsgBase, changeLog) ;
    }
    
    public static void write(OutputStream out, RDFPatch patch) {
        TokenWriter tw = new TokenWriterText(out) ; 
        RDFChanges c = new RDFChangesWriter(tw) ;
        patch.apply(c);
        tw.flush();
    }
}
